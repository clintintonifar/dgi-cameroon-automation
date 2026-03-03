# =============================================================================
# DGI Cameroon - GitHub Actions Automation (OAuth Version)
# Rolling 5-Year Window: Always keep last 5 full years of data
# The first year is always COMPLETE (starts at January of current_year - 5)
# Output: DGI_COMBINED.parquet (~300-500MB vs 1.5GB CSV)
# Uses OAuth Refresh Token (Personal Account Quota)
# ALL credentials loaded from GitHub Secrets (environment variables)
#
# Optimizations applied:
#   [1] PyArrow ParquetWriter — stream-writes parquet incrementally,
#       no full DataFrame held in RAM, no giant concat at the end
#   [2] calamine engine — Rust-based Excel reader, 3-6x faster than openpyxl
#   [3] DOWNLOAD_WORKERS = 3 — optimal for GitHub Actions 2-core free runners
#   [4] requests timeout = 20s — faster fail + retry, not 60s hanging
#   [5] Drive list() with pagination — safe when folder grows large
#
# Drive upload strategy:
#   uses files().update() to replace content IN-PLACE on every run,
#   preserving the same file ID and public sharing link forever.
#   Only uses files().create() on the very first upload.
#
# Naming-convention fix (v3):
#   The DGI site uses inconsistent separators between FICHIER / MONTH / YEAR.
#   build_candidate_urls() tries all 16 variants (8 separator combos × 2 cases).
#
# Smart sentinel check (v5):
#   After a successful download, writes last_downloaded.txt to the repo root
#   (committed by the workflow via git). On the next scheduled run, the YAML
#   checks this file BEFORE installing Python — if the current expected month
#   is already recorded, the job is cancelled in ~5 seconds at zero cost.
#   The Python script also re-checks internally as a safety net.
#
# Email notifications (v4):
#   Sends a summary email (to NOTIFY_EMAIL) via SMTP after every run.
#   Supports Gmail (smtp.gmail.com:587) and Outlook (smtp.office365.com:587).
# =============================================================================

import os
import sys
import time
import smtplib
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import random

# =============================================================================
# CONFIGURATION
# =============================================================================
YEARS_TO_KEEP      = 5
DOWNLOAD_WORKERS   = 3
MAX_RETRIES        = 5
RETRY_DELAY        = 5
REQUEST_TIMEOUT    = 20

# Path to the sentinel file — committed to repo root by the workflow after
# a successful download so subsequent runs in the same month skip instantly.
SENTINEL_FILE = os.path.join(
    os.environ.get('GITHUB_WORKSPACE', '.'),
    'last_downloaded.txt'
)

FRENCH_MONTHS = {
    1: 'JANVIER', 2: 'FEVRIER', 3: 'MARS', 4: 'AVRIL',
    5: 'MAI', 6: 'JUIN', 7: 'JUILLET', 8: 'AOUT',
    9: 'SEPTEMBRE', 10: 'OCTOBRE', 11: 'NOVEMBRE', 12: 'DECEMBRE'
}

CANONICAL_COLUMNS = [
    'RAISON_SOCIALE', 'SIGLE', 'NIU', 'ACTIVITE_PRINCIPALE',
    'REGIME', 'CRI', 'CENTRE_DE_RATTACHEMENT',
]

PARQUET_SCHEMA = pa.schema([
    pa.field('YEAR',                   pa.int16()),
    pa.field('MONTH',                  pa.int16()),
    pa.field('RAISON_SOCIALE',         pa.string()),
    pa.field('SIGLE',                  pa.string()),
    pa.field('NIU',                    pa.string()),
    pa.field('ACTIVITE_PRINCIPALE',    pa.string()),
    pa.field('REGIME',                 pa.string()),
    pa.field('CRI',                    pa.string()),
    pa.field('CENTRE_DE_RATTACHEMENT', pa.string()),
])

BASE_HOST        = "https://teledeclaration-dgi.cm/UploadedFiles/AttachedFiles/ArchiveListecontribuable"
DOWNLOAD_DIR     = "/tmp/dgi_downloads"
COMBINED_PARQUET = "/tmp/dgi_downloads/DGI_COMBINED.parquet"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# =============================================================================
# SENTINEL FILE — tracks the last successfully downloaded month
# =============================================================================

def get_expected_latest_month() -> tuple:
    """
    Return (year, month) of the most recent file that should exist on DGI.
    DGI publishes month M data during month M+1, so expected latest = previous month.
    e.g. running in February 2026 → expected latest = January 2026
    """
    now = datetime.now()
    if now.month == 1:
        return (now.year - 1, 12)
    return (now.year, now.month - 1)


def sentinel_month_key(year: int, month: int) -> str:
    """Canonical string stored in sentinel file — e.g. '2026-01'"""
    return f"{year}-{month:02d}"


def read_sentinel() -> str:
    """Read sentinel file. Returns empty string if it doesn't exist."""
    if os.path.exists(SENTINEL_FILE):
        with open(SENTINEL_FILE, 'r') as f:
            return f.read().strip()
    return ''


def write_sentinel(year: int, month: int):
    """
    Write successfully-downloaded month to the sentinel file.
    The workflow's git commit step pushes this back to the repo so the
    next scheduled run sees it immediately and exits in ~5 seconds.
    """
    key = sentinel_month_key(year, month)
    with open(SENTINEL_FILE, 'w') as f:
        f.write(key + '\n')
    print(f"  📝 Sentinel written: {SENTINEL_FILE} → '{key}'")


def sentinel_already_downloaded() -> bool:
    """True if sentinel already records the current expected month."""
    expected_year, expected_month = get_expected_latest_month()
    expected_key = sentinel_month_key(expected_year, expected_month)
    return read_sentinel() == expected_key

# =============================================================================
# EMAIL NOTIFICATION
# =============================================================================

def send_email_notification(subject: str, body: str, status: str = "info"):
    """
    Send a run summary email via SMTP.

    Required GitHub Secrets → env vars:
        SMTP_HOST      — smtp.gmail.com  OR  smtp.office365.com
        SMTP_PORT      — 587 (STARTTLS)
        SMTP_USER      — your full email address (sender)
        SMTP_PASSWORD  — Gmail App Password or Outlook password
        NOTIFY_EMAIL   — recipient address (can be same as SMTP_USER)
    """
    smtp_host = os.environ.get('SMTP_HOST', '')
    smtp_port = int(os.environ.get('SMTP_PORT', '587'))
    smtp_user = os.environ.get('SMTP_USER', '')
    smtp_pass = os.environ.get('SMTP_PASSWORD', '')
    notify_to = os.environ.get('NOTIFY_EMAIL', smtp_user)

    if not all([smtp_host, smtp_user, smtp_pass, notify_to]):
        print("  ⚠️  Email config incomplete — skipping notification")
        print("      Set SMTP_HOST, SMTP_USER, SMTP_PASSWORD, NOTIFY_EMAIL secrets.")
        return

    emoji = {'success': '✅', 'warning': '⚠️', 'error': '❌'}.get(status, 'ℹ️')
    full_subject = f"{emoji} DGI Cameroon Pipeline — {subject}"

    html_body = f"""
    <html><body style="font-family: monospace; font-size: 13px; color: #222;">
    <h2 style="color: {'#2a7a2a' if status == 'success' else '#c0392b' if status == 'error' else '#e67e22'};">
        {emoji} DGI Cameroon Pipeline Report
    </h2>
    <pre style="background:#f4f4f4; padding:16px; border-radius:6px; white-space:pre-wrap;">{body}</pre>
    <hr/>
    <small style="color:#888;">
        Sent automatically by the DGI GitHub Actions pipeline.<br/>
        Run time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S UTC')}
    </small>
    </body></html>
    """

    msg = MIMEMultipart('alternative')
    msg['Subject'] = full_subject
    msg['From']    = smtp_user
    msg['To']      = notify_to
    msg.attach(MIMEText(body,      'plain'))
    msg.attach(MIMEText(html_body, 'html'))

    try:
        with smtplib.SMTP(smtp_host, smtp_port, timeout=30) as server:
            server.ehlo()
            server.starttls()
            server.ehlo()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, notify_to, msg.as_string())
        print(f"  📧 Notification sent → {notify_to}")
    except Exception as e:
        print(f"  ⚠️  Email send failed: {e}")

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def parse_filename_to_date(filename):
    """Extract (year, month) from 'FICHIER_JANVIER_2026.xlsx'"""
    try:
        clean = filename.replace("FICHIER_", "").replace(".xlsx", "")
        parts = clean.split("_")
        if len(parts) != 2:
            return None
        month_name, year_str = parts
        year = int(year_str)
        for num, name in FRENCH_MONTHS.items():
            if name == month_name:
                return (year, num)
        return None
    except Exception:
        return None


def probe_latest_file_available() -> bool:
    """
    Probe the DGI website to confirm the expected latest file is published.
    Uses HEAD requests (zero bandwidth) across all URL variants.
    Returns True if any variant returns HTTP 200.
    """
    expected_year, expected_month = get_expected_latest_month()
    month_name = FRENCH_MONTHS[expected_month]

    print(f"\n🔍 Probing DGI site for: FICHIER_*{month_name}*{expected_year}.xlsx ...")

    candidate_urls = build_candidate_urls(month_name, expected_year)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    }

    for url in candidate_urls:
        try:
            resp = requests.head(url, headers=headers, timeout=REQUEST_TIMEOUT,
                                 allow_redirects=True)
            if resp.status_code == 200:
                print(f"  ✅ File confirmed available at: {url}")
                return True
            if resp.status_code == 405:
                resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT, stream=True)
                resp.close()
                if resp.status_code == 200:
                    print(f"  ✅ File confirmed available at: {url}")
                    return True
        except Exception:
            continue

    print(f"  ℹ️  {month_name} {expected_year} not yet published on DGI site.")
    return False


def get_month_list():
    """
    Generate (year, month) tuples for the full 5-year window.
    Upper bound = previous month (current month's data isn't published yet).
    """
    now = datetime.now()
    start_year, start_month = now.year - YEARS_TO_KEEP, 1
    end_year, end_month = get_expected_latest_month()

    months = []
    year, month = start_year, start_month
    while (year, month) <= (end_year, end_month):
        months.append((year, month))
        month += 1
        if month > 12:
            month = 1
            year += 1
    return months

# =============================================================================
# URL CANDIDATE BUILDER
# =============================================================================

def build_candidate_urls(month_name: str, year: int) -> list:
    """Return every plausible URL variant for one month's file."""
    separators = [
        ('%20',  '%20'), ('_',    '%20'), ('%20',  '_'),   ('_',    '_'),
        ('%20_', '%20'), ('_%20', '%20'), ('%20_', '_'),   ('_%20', '_'),
    ]
    month_variants = [month_name, month_name.capitalize()]
    urls = []
    for mv in month_variants:
        for s1, s2 in separators:
            urls.append(f"{BASE_HOST}/FICHIER{s1}{mv}{s2}{year}.xlsx")
    return urls

# =============================================================================
# DOWNLOAD
# =============================================================================

def download_file(year, month):
    month_name = FRENCH_MONTHS[month]
    filename   = f"FICHIER_{month_name}_{year}.xlsx"
    filepath   = os.path.join(DOWNLOAD_DIR, filename)

    if os.path.exists(filepath):
        return year, month, 'skipped'

    candidate_urls = build_candidate_urls(month_name, year)
    headers = {
        "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Accept":          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, application/octet-stream, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection":      "keep-alive",
        "Cache-Control":   "no-cache",
    }
    last_status = 'not_found'

    for url in candidate_urls:
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
                if response.status_code == 200:
                    with open(filepath, 'wb') as f:
                        f.write(response.content)
                    return year, month, 'downloaded'
                elif response.status_code == 404:
                    last_status = 'not_found'
                    break
                else:
                    last_status = 'failed'
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY + random.uniform(1, 3))
            except Exception:
                last_status = 'failed'
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY + random.uniform(1, 3))

    return year, month, last_status


def download_all_parallel(months_to_process):
    downloaded = skipped = failed = 0
    failed_files = []
    total = len(months_to_process)

    print(f"\n📥 Downloading {total} months ({DOWNLOAD_WORKERS} parallel threads)...")

    with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as executor:
        futures = {
            executor.submit(download_file, year, month): (year, month)
            for year, month in months_to_process
        }
        completed = 0
        for future in as_completed(futures):
            year, month, status = future.result()
            month_name = FRENCH_MONTHS[month]
            completed += 1
            label = f"  [{completed}/{total}]"

            if status == 'downloaded':
                downloaded += 1
                print(f"{label} ✓ Downloaded: FICHIER_{month_name}_{year}.xlsx")
            elif status == 'skipped':
                skipped += 1
                print(f"{label} → Skip: FICHIER_{month_name}_{year}.xlsx (exists)")
            elif status == 'not_found':
                failed += 1
                failed_files.append(f"FICHIER_{month_name}_{year}.xlsx (not found)")
                print(f"{label} ⚠ Not found: FICHIER_{month_name}_{year}.xlsx")
            else:
                failed += 1
                failed_files.append(f"FICHIER_{month_name}_{year}.xlsx (error)")
                print(f"{label} ✗ Failed: FICHIER_{month_name}_{year}.xlsx")

    return downloaded, skipped, failed, failed_files

# =============================================================================
# COLUMN NORMALIZATION
# =============================================================================

def normalize_to_arrow_table(df, year, month):
    df.columns = [str(c).strip().upper() for c in df.columns]
    for drop_col in ['N°', 'N', 'Nº']:
        if drop_col in df.columns:
            df.drop(columns=[drop_col], inplace=True)
    for col in CANONICAL_COLUMNS:
        if col not in df.columns:
            df[col] = ''
    df = df[CANONICAL_COLUMNS].copy()
    for col in CANONICAL_COLUMNS:
        df[col] = df[col].astype(str).str.strip().replace('nan', '')
    n = len(df)
    df.insert(0, 'MONTH', pd.array([month] * n, dtype='int16'))
    df.insert(0, 'YEAR',  pd.array([year]  * n, dtype='int16'))
    return pa.Table.from_pandas(df, schema=PARQUET_SCHEMA, preserve_index=False)

# =============================================================================
# COMBINE — INCREMENTAL PARQUET STREAMING
# =============================================================================

def combine_to_parquet(newly_downloaded):
    if newly_downloaded == 0 and os.path.exists(COMBINED_PARQUET):
        size_mb = os.path.getsize(COMBINED_PARQUET) / 1024 / 1024
        print(f"\n📊 No new files — reusing existing Parquet ({size_mb:.1f} MB)")
        return COMBINED_PARQUET

    print("\n📊 Building Parquet incrementally (PyArrow streaming writer)...")

    xlsx_files = sorted([
        f for f in os.listdir(DOWNLOAD_DIR)
        if f.endswith('.xlsx') and f.startswith('FICHIER_')
    ])

    if not xlsx_files:
        print("  ⚠️ No Excel files found to combine.")
        return None

    try:
        import python_calamine  # noqa: F401
        excel_engine = 'calamine'
        print("  ⚡ Excel engine: calamine (Rust-based)")
    except ImportError:
        excel_engine = 'openpyxl'
        print("  ⚠️ Excel engine: openpyxl (install python-calamine for 3-6x speedup)")

    total_rows = 0

    with pq.ParquetWriter(
        COMBINED_PARQUET, schema=PARQUET_SCHEMA,
        compression='snappy', use_dictionary=True, write_statistics=True,
    ) as writer:
        for i, filename in enumerate(xlsx_files, 1):
            parsed = parse_filename_to_date(filename)
            if parsed is None:
                print(f"  ⚠ Skipping (unparseable name): {filename}")
                continue
            year, month = parsed
            filepath = os.path.join(DOWNLOAD_DIR, filename)
            try:
                df = pd.read_excel(filepath, sheet_name=0, dtype=str, engine=excel_engine)
                df.dropna(how='all', inplace=True)
                arrow_table = normalize_to_arrow_table(df, year, month)
                writer.write_table(arrow_table, row_group_size=100_000)
                total_rows += len(arrow_table)
                print(f"  [{i}/{len(xlsx_files)}] ✓ {filename} — {len(arrow_table):,} rows")
            except Exception as e:
                print(f"  [{i}/{len(xlsx_files)}] ✗ Failed: {filename} — {str(e)}")

    if total_rows == 0:
        print("  ✗ No data written.")
        os.remove(COMBINED_PARQUET)
        return None

    size_mb = os.path.getsize(COMBINED_PARQUET) / 1024 / 1024
    print(f"\n  ✅ Parquet ready: {total_rows:,} rows, {size_mb:.1f} MB")
    return COMBINED_PARQUET

# =============================================================================
# GOOGLE DRIVE
# =============================================================================

def authenticate_drive():
    try:
        print("  🔍 Checking env vars:")
        for var in ['GOOGLE_REFRESH_TOKEN', 'GOOGLE_CLIENT_ID', 'GOOGLE_CLIENT_SECRET']:
            status = '✅ Set' if os.environ.get(var) else '❌ Missing'
            print(f"     {var}: {status}")

        refresh_token = os.environ.get('GOOGLE_REFRESH_TOKEN')
        client_id     = os.environ.get('GOOGLE_CLIENT_ID')
        client_secret = os.environ.get('GOOGLE_CLIENT_SECRET')

        if not all([refresh_token, client_id, client_secret]):
            print("  ⚠️ Missing credentials — skipping upload")
            return None

        creds = Credentials(
            None, refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id, client_secret=client_secret,
            scopes=['https://www.googleapis.com/auth/drive'],
        )
        service = build('drive', 'v3', credentials=creds)
        print("  ✅ Google Drive authenticated via OAuth")
        return service
    except Exception as e:
        print(f"  ⚠️ Drive auth failed: {str(e)}")
        return None


def list_drive_files(service, folder_id, mime_type=None):
    query = f"'{folder_id}' in parents"
    if mime_type:
        query += f" and mimeType='{mime_type}'"
    files, page_token = [], None
    while True:
        kwargs = {
            'q': query, 'fields': 'nextPageToken, files(id, name)',
            'supportsAllDrives': True, 'pageSize': 1000,
        }
        if page_token:
            kwargs['pageToken'] = page_token
        result     = service.files().list(**kwargs).execute()
        files     += result.get('files', [])
        page_token = result.get('nextPageToken')
        if not page_token:
            break
    return files


def upload_to_drive(service, drive_folder_id):
    if not service or not os.path.exists(COMBINED_PARQUET):
        print("  ⚠️ Skipping upload (no service or file missing)")
        return 0

    parquet_name = "DGI_COMBINED.parquet"
    try:
        existing_files = [
            f for f in list_drive_files(service, drive_folder_id)
            if f['name'] == parquet_name
        ]
        media = MediaFileUpload(COMBINED_PARQUET, mimetype='application/octet-stream', resumable=True)

        if existing_files:
            existing_id = existing_files[0]['id']
            for duplicate in existing_files[1:]:
                service.files().delete(fileId=duplicate['id'], supportsAllDrives=True).execute()
                print(f"  🗑 Removed duplicate (ID: {duplicate['id']})")
            service.files().update(fileId=existing_id, media_body=media, supportsAllDrives=True).execute()
            print(f"  ✓ Updated in-place: {parquet_name} (file ID & public link preserved ✅)")
        else:
            service.files().create(
                body={'name': parquet_name, 'parents': [drive_folder_id]},
                media_body=media, fields='id', supportsAllDrives=True,
            ).execute()
            print(f"  ✓ Created new: {parquet_name} (first upload)")
            print(f"  ℹ️  Share this file's public link now — it will never change.")
        return 1
    except Exception as e:
        print(f"  ✗ Upload failed: {str(e)}")
        return 0


def cleanup_old_files(service, drive_folder_id, cutoff_date):
    if not service:
        return 0, 0
    deleted = kept = 0
    try:
        xlsx_mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        for file in list_drive_files(service, drive_folder_id, mime_type=xlsx_mime):
            filename = file['name']
            parsed   = parse_filename_to_date(filename)
            if parsed:
                year, month = parsed
                if datetime(year, month, 1) >= cutoff_date:
                    kept += 1
                else:
                    service.files().delete(fileId=file['id'], supportsAllDrives=True).execute()
                    print(f"  🗑 Deleted old: {filename}")
                    deleted += 1
            else:
                kept += 1
    except Exception as e:
        print(f"  ⚠️ Cleanup error: {str(e)}")
    return kept, deleted

# =============================================================================
# MAIN
# =============================================================================

def main():
    start_time = datetime.now()
    run_log    = []

    def log(msg):
        print(msg)
        run_log.append(msg)

    log("=" * 70)
    log("DGI Cameroon - GitHub Actions Automation (OAuth + Sentinel)")
    log(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    log("=" * 70)

    DRIVE_FOLDER_ID = os.environ.get('DRIVE_FOLDER_ID', '')
    now             = datetime.now()
    cutoff_date     = datetime(now.year - YEARS_TO_KEEP, 1, 1)
    expected_year, expected_month = get_expected_latest_month()
    expected_month_name = FRENCH_MONTHS[expected_month]

    log(f"📅 Data window: {cutoff_date.strftime('%Y-%m')} → "
        f"{expected_year}-{expected_month:02d} (latest expected)")
    log(f"📄 Sentinel file : {SENTINEL_FILE}")
    log(f"   Current value : '{read_sentinel()}'")
    log(f"   Expected key  : '{sentinel_month_key(expected_year, expected_month)}'")

    # -------------------------------------------------------------------------
    # SENTINEL CHECK (Python-side safety net — primary check is in the YAML)
    # -------------------------------------------------------------------------
    if sentinel_already_downloaded():
        msg = (f"ℹ️  Sentinel confirms {expected_month_name} {expected_year} was "
               f"already downloaded this month. Nothing to do.")
        log(msg)
        send_email_notification(
            subject=f"Skipped — {expected_month_name} {expected_year} already downloaded",
            body="\n".join(run_log),
            status="info",
        )
        sys.exit(0)

    # -------------------------------------------------------------------------
    # PROBE DGI SITE
    # -------------------------------------------------------------------------
    if not probe_latest_file_available():
        log(f"ℹ️  {expected_month_name} {expected_year} not yet on DGI site. "
            f"Will retry next scheduled run.")
        send_email_notification(
            subject=f"Skipped — {expected_month_name} {expected_year} not yet published",
            body="\n".join(run_log),
            status="warning",
        )
        sys.exit(0)

    log(f"✅ New file confirmed available — proceeding with full pipeline.\n")

    # -------------------------------------------------------------------------
    # FULL PIPELINE
    # -------------------------------------------------------------------------
    months_to_process = get_month_list()
    log(f"📊 Will process {len(months_to_process)} months")

    downloaded, skipped, failed, failed_files = download_all_parallel(months_to_process)

    parquet_path = combine_to_parquet(newly_downloaded=downloaded)

    log("\n🔐 Authenticating Google Drive...")
    drive_service = authenticate_drive()

    uploaded = kept = deleted = 0
    if drive_service and DRIVE_FOLDER_ID:
        log("\n📤 Uploading to Google Drive...")
        uploaded = upload_to_drive(drive_service, DRIVE_FOLDER_ID)
        log(f"   Uploaded: {uploaded} files")

        log("\n🧹 Cleaning up files older than 5 years...")
        kept, deleted = cleanup_old_files(drive_service, DRIVE_FOLDER_ID, cutoff_date)
        log(f"   Kept: {kept}, Deleted: {deleted}")
    else:
        log("\n⚠️ Skipping Drive operations (no credentials or folder ID)")

    # -------------------------------------------------------------------------
    # WRITE SENTINEL — only after the expected latest file is confirmed on disk
    # -------------------------------------------------------------------------
    latest_filename = f"FICHIER_{expected_month_name}_{expected_year}.xlsx"
    latest_path     = os.path.join(DOWNLOAD_DIR, latest_filename)

    if os.path.exists(latest_path):
        write_sentinel(expected_year, expected_month)
        log(f"\n✅ Sentinel updated → '{sentinel_month_key(expected_year, expected_month)}'")
        log("   (Workflow will commit this to the repo to cancel remaining runs this month)")
    else:
        log(f"\n⚠️  Latest file ({latest_filename}) not on disk — sentinel NOT updated.")
        log("   Next scheduled run will try again.")

    # -------------------------------------------------------------------------
    # SUMMARY + EMAIL
    # -------------------------------------------------------------------------
    end_time   = datetime.now()
    duration   = end_time - start_time
    parquet_mb = (os.path.getsize(parquet_path) / 1024 / 1024
                  if parquet_path and os.path.exists(parquet_path) else 0)

    summary_lines = [
        "",
        "=" * 70,
        "✅ EXECUTION COMPLETE",
        f"   Run duration:  {duration}",
        f"   Downloaded:    {downloaded} new files",
        f"   Skipped:       {skipped} (already existed)",
        f"   Failed:        {failed} (not found or error)",
        f"   Parquet:       {parquet_mb:.1f} MB",
        f"   Drive upload:  {uploaded} file(s)",
        f"   Drive cleanup: kept {kept}, deleted {deleted}",
        f"Finished: {end_time.strftime('%Y-%m-%d %H:%M:%S UTC')}",
        "=" * 70,
    ]
    if failed_files:
        summary_lines += ["", "⚠️  Files that could not be downloaded:"]
        summary_lines += [f"   • {f}" for f in failed_files]

    for line in summary_lines:
        log(line)

    overall_status = (
        "error"   if failed > downloaded else
        "success" if downloaded > 0     else
        "warning"
    )
    send_email_notification(
        subject=(f"Run complete — {downloaded} new, {skipped} skipped, "
                 f"{failed} failed ({expected_month_name} {expected_year})"),
        body="\n".join(run_log),
        status=overall_status,
    )


if __name__ == "__main__":
    main()