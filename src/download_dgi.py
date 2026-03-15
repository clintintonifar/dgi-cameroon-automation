# =============================================================================
# DGI Cameroon - GitHub Actions Automation (OAuth Version)
# Rolling 5-Year Window: Always keep last 5 full years of data
#
# OUTPUT STRATEGY — 2 tables (Power BI optimised, ~80% size reduction):
#
#   DGI_CURRENT.parquet   (~533k rows, ~40 MB)
#     └─ Full taxpayer snapshot for the LATEST month only.
#        Use for: company lookup, name search, slicer dimensions
#        (REGIME, CRI, CENTRE_DE_RATTACHEMENT).
#
#   DGI_PRESENCE.parquet  (~17M rows, ~60 MB)
#     └─ One row per (NIU, YEAR, MONTH) — only 3 narrow columns.
#        NIU is dictionary-encoded so 17M rows compress to ~60 MB.
#        Powers ALL visuals: heatmap, trend charts, KPIs, new registrations.
#        Absent (NIU, YEAR, MONTH) = taxpayer was inactive that month.
#
#   Relationship: DGI_CURRENT[NIU] → DGI_PRESENCE[NIU]  (1-to-many, both)
#   TOTAL: ~100 MB in Power BI  vs  660 MB previously.
#
# Build strategy:
#   Single pass over all Excel files in chronological order.
#   PRESENCE written incrementally via streaming ParquetWriter (no concat).
#   Only one month's DataFrame is in RAM at a time (~120 MB peak).
#
# All other infrastructure (sentinel, email, Drive upload) is unchanged.
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
from datetime import datetime
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import random

# =============================================================================
# CONFIGURATION
# =============================================================================
YEARS_TO_KEEP    = 5
DOWNLOAD_WORKERS = 3
MAX_RETRIES      = 5
RETRY_DELAY      = 5
REQUEST_TIMEOUT  = 20

SENTINEL_FILE = os.path.join(
    os.environ.get('GITHUB_WORKSPACE', '.'),
    'last_downloaded.txt'
)

FRENCH_MONTHS = {
    1: 'JANVIER', 2: 'FEVRIER', 3: 'MARS',    4: 'AVRIL',
    5: 'MAI',     6: 'JUIN',    7: 'JUILLET',  8: 'AOUT',
    9: 'SEPTEMBRE', 10: 'OCTOBRE', 11: 'NOVEMBRE', 12: 'DECEMBRE'
}

# Reverse lookup: French month name → month number
FRENCH_MONTHS_REVERSE = {v: k for k, v in FRENCH_MONTHS.items()}

CANONICAL_COLUMNS = [
    'RAISON_SOCIALE', 'SIGLE', 'NIU', 'ACTIVITE_PRINCIPALE',
    'REGIME', 'CRI', 'CENTRE_DE_RATTACHEMENT',
]

# ── Parquet schemas ──────────────────────────────────────────────────────────

# Current-month full snapshot (Power BI: lookup / drill-through)
CURRENT_SCHEMA = pa.schema([
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

# Per-taxpayer monthly presence (Power BI: all visuals)
# 3 narrow columns — NIU dictionary-encoded → ~60 MB for 17M rows
PRESENCE_SCHEMA = pa.schema([
    pa.field('NIU',   pa.dictionary(pa.int32(), pa.string())),
    pa.field('YEAR',  pa.int16()),
    pa.field('MONTH', pa.int16()),
])

BASE_HOST        = "https://teledeclaration-dgi.cm/UploadedFiles/AttachedFiles/ArchiveListecontribuable"
DOWNLOAD_DIR     = "/tmp/dgi_downloads"
CURRENT_PARQUET  = "/tmp/dgi_downloads/DGI_CURRENT.parquet"
PRESENCE_PARQUET = "/tmp/dgi_downloads/DGI_PRESENCE.parquet"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# =============================================================================
# SENTINEL
# =============================================================================

def get_expected_latest_month() -> tuple:
    now = datetime.now()
    if now.month == 1:
        return (now.year - 1, 12)
    return (now.year, now.month - 1)

def sentinel_month_key(year: int, month: int) -> str:
    return f"{year}-{month:02d}"

def read_sentinel() -> str:
    if os.path.exists(SENTINEL_FILE):
        with open(SENTINEL_FILE, 'r') as f:
            return f.read().strip()
    return ''

def write_sentinel(year: int, month: int):
    key = sentinel_month_key(year, month)
    with open(SENTINEL_FILE, 'w') as f:
        f.write(key + '\n')
    print(f"  📝 Sentinel written: {SENTINEL_FILE} → '{key}'")

def sentinel_already_downloaded() -> bool:
    expected_year, expected_month = get_expected_latest_month()
    return read_sentinel() == sentinel_month_key(expected_year, expected_month)

# =============================================================================
# EMAIL NOTIFICATION
# =============================================================================

def send_email_notification(subject: str, body: str, status: str = "info"):
    smtp_host = os.environ.get('SMTP_HOST', '')
    smtp_port = int(os.environ.get('SMTP_PORT', '587'))
    smtp_user = os.environ.get('SMTP_USER', '')
    smtp_pass = os.environ.get('SMTP_PASSWORD', '')
    notify_to = os.environ.get('NOTIFY_EMAIL', smtp_user)

    if not all([smtp_host, smtp_user, smtp_pass, notify_to]):
        print("  ⚠️  Email config incomplete — skipping notification")
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
            server.ehlo(); server.starttls(); server.ehlo()
            server.login(smtp_user, smtp_pass)
            server.sendmail(smtp_user, notify_to, msg.as_string())
        print(f"  📧 Notification sent → {notify_to}")
    except Exception as e:
        print(f"  ⚠️  Email send failed: {e}")

# =============================================================================
# HELPERS
# =============================================================================

def parse_filename_to_date(filename: str):
    """Extract (year, month) from 'FICHIER_JANVIER_2026.xlsx'"""
    try:
        clean = filename.replace("FICHIER_", "").replace(".xlsx", "")
        parts = clean.split("_")
        if len(parts) != 2:
            return None
        month_name, year_str = parts
        year = int(year_str)
        month = FRENCH_MONTHS_REVERSE.get(month_name.upper())
        return (year, month) if month else None
    except Exception:
        return None


def get_month_list():
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


def probe_latest_file_available() -> bool:
    expected_year, expected_month = get_expected_latest_month()
    month_name = FRENCH_MONTHS[expected_month]
    print(f"\n🔍 Probing DGI site for: FICHIER_*{month_name}*{expected_year}.xlsx ...")
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
    }
    for url in build_candidate_urls(month_name, expected_year):
        try:
            resp = requests.head(url, headers=headers, timeout=REQUEST_TIMEOUT, allow_redirects=True)
            if resp.status_code == 200:
                print(f"  ✅ File confirmed at: {url}")
                return True
            if resp.status_code == 405:
                resp = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT, stream=True)
                resp.close()
                if resp.status_code == 200:
                    print(f"  ✅ File confirmed at: {url}")
                    return True
        except Exception:
            continue
    print(f"  ℹ️  {month_name} {expected_year} not yet published.")
    return False

# =============================================================================
# URL CANDIDATE BUILDER
# =============================================================================

def build_candidate_urls(month_name: str, year: int) -> list:
    separators = [
        ('%20',  '%20'), ('_',    '%20'), ('%20',  '_'),   ('_',    '_'),
        ('%20_', '%20'), ('_%20', '%20'), ('%20_', '_'),   ('_%20', '_'),
    ]
    urls = []
    for mv in [month_name, month_name.capitalize()]:
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

    headers = {
        "User-Agent":      "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Accept":          "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, application/octet-stream, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection":      "keep-alive",
        "Cache-Control":   "no-cache",
    }

    for url in build_candidate_urls(month_name, year):
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(url, headers=headers, timeout=REQUEST_TIMEOUT)
                if response.status_code == 200:
                    with open(filepath, 'wb') as f:
                        f.write(response.content)
                    return year, month, 'downloaded'
                elif response.status_code == 404:
                    break
                else:
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY + random.uniform(1, 3))
            except Exception:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY + random.uniform(1, 3))

    return year, month, 'not_found'


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
                print(f"{label} → Skip (exists): FICHIER_{month_name}_{year}.xlsx")
            else:
                failed += 1
                failed_files.append(f"FICHIER_{month_name}_{year}.xlsx")
                print(f"{label} ⚠ Not found: FICHIER_{month_name}_{year}.xlsx")

    return downloaded, skipped, failed, failed_files

# =============================================================================
# COLUMN NORMALISATION
# =============================================================================

def normalize_df(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise raw Excel DataFrame to canonical columns with clean strings."""
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
    return df

# =============================================================================
# BUILD CURRENT + PRESENCE PARQUETS
# =============================================================================

def build_parquets(newly_downloaded: int) -> tuple:
    """
    Build both Power BI output files in a single chronological pass.

    DGI_CURRENT.parquet  — latest month full snapshot  (~533k rows, ~40 MB)
    DGI_PRESENCE.parquet — NIU × YEAR × MONTH presence (~17M rows, ~60 MB)

    Memory strategy
    ───────────────
    Only ONE month's DataFrame is in RAM at a time.
    PRESENCE is written incrementally via streaming ParquetWriter — no
    accumulation, no giant concat. Peak RAM ≈ one month (~120 MB).

    Skips rebuild if both files already exist and nothing new was downloaded.
    """
    both_exist = (
        os.path.exists(CURRENT_PARQUET) and
        os.path.exists(PRESENCE_PARQUET)
    )
    if newly_downloaded == 0 and both_exist:
        cur_mb = os.path.getsize(CURRENT_PARQUET)  / 1024 / 1024
        pre_mb = os.path.getsize(PRESENCE_PARQUET) / 1024 / 1024
        print(f"\n📊 No new files — reusing existing Parquets "
              f"(Current: {cur_mb:.1f} MB, Presence: {pre_mb:.1f} MB)")
        return CURRENT_PARQUET, PRESENCE_PARQUET

    print("\n📊 Building CURRENT + PRESENCE (single pass)...")

    # ── Collect and sort files chronologically ───────────────────────────────
    dated_files = []
    for filename in os.listdir(DOWNLOAD_DIR):
        if not (filename.endswith('.xlsx') and filename.startswith('FICHIER_')):
            continue
        parsed = parse_filename_to_date(filename)
        if parsed:
            dated_files.append((parsed[0], parsed[1], filename))
        else:
            print(f"  ⚠ Skipping unparseable file: {filename}")

    if not dated_files:
        print("  ⚠ No valid Excel files found.")
        return None, None

    dated_files.sort(key=lambda x: (x[0], x[1]))
    total = len(dated_files)
    latest_year, latest_month, _ = dated_files[-1]

    # ── Excel engine ─────────────────────────────────────────────────────────
    try:
        import python_calamine  # noqa
        excel_engine = 'calamine'
        print("  ⚡ Excel engine: calamine (Rust-based, 3-6× faster)")
    except ImportError:
        excel_engine = 'openpyxl'
        print("  ⚠ Excel engine: openpyxl (install python-calamine for speedup)")

    total_presence_rows = 0

    # ── Open streaming PRESENCE writer — stays open for the entire loop ──────
    with pq.ParquetWriter(
        PRESENCE_PARQUET, schema=PRESENCE_SCHEMA,
        compression='snappy',
        use_dictionary=True,   # key: dictionary-encodes NIU → ~60 MB for 17M rows
        write_statistics=True,
    ) as presence_writer:

        for i, (year, month, filename) in enumerate(dated_files, 1):
            filepath = os.path.join(DOWNLOAD_DIR, filename)
            try:
                raw = pd.read_excel(filepath, sheet_name=0, dtype=str, engine=excel_engine)
                raw.dropna(how='all', inplace=True)
                df  = normalize_df(raw)
            except Exception as e:
                print(f"  [{i}/{total}] ✗ Failed to read {filename}: {e}")
                continue

            df_valid = df[df['NIU'] != ''].copy()

            # ── Write presence rows for this month ───────────────────────────
            # pd.Categorical tells PyArrow to use dictionary encoding on NIU
            n = len(df_valid)
            presence_table = pa.Table.from_pandas(
                pd.DataFrame({
                    'NIU':   pd.Categorical(df_valid['NIU']),
                    'YEAR':  pd.array([year]  * n, dtype='int16'),
                    'MONTH': pd.array([month] * n, dtype='int16'),
                }),
                schema=PRESENCE_SCHEMA,
                preserve_index=False,
            )
            presence_writer.write_table(presence_table, row_group_size=200_000)
            total_presence_rows += n

            # ── Write CURRENT for the latest month only ──────────────────────
            if year == latest_year and month == latest_month:
                df_current = df_valid.copy()
                df_current.insert(0, 'MONTH', pd.array([month] * n, dtype='int16'))
                df_current.insert(0, 'YEAR',  pd.array([year]  * n, dtype='int16'))
                pq.write_table(
                    pa.Table.from_pandas(df_current, schema=CURRENT_SCHEMA, preserve_index=False),
                    CURRENT_PARQUET,
                    compression='snappy', use_dictionary=True, write_statistics=True,
                )
                cur_mb = os.path.getsize(CURRENT_PARQUET) / 1024 / 1024
                print(f"  [{i}/{total}] ✓ {filename} — {n:,} rows → CURRENT ({cur_mb:.1f} MB) + PRESENCE")
            else:
                print(f"  [{i}/{total}] ✓ {filename} — {n:,} rows → PRESENCE")

            # Free this month from RAM before loading the next one
            del df, df_valid, raw, presence_table

    # PRESENCE writer flushed and closed here

    cur_mb = os.path.getsize(CURRENT_PARQUET)  / 1024 / 1024 if os.path.exists(CURRENT_PARQUET) else 0
    pre_mb = os.path.getsize(PRESENCE_PARQUET) / 1024 / 1024

    print(f"\n  ✅ CURRENT  : {cur_mb:.1f} MB  (~{latest_year}-{latest_month:02d} snapshot)")
    print(f"  ✅ PRESENCE : {pre_mb:.1f} MB  ({total_presence_rows:,} rows)")
    print(f"\n  📉 Total in Power BI: {cur_mb + pre_mb:.1f} MB  (was 660 MB)")

    return CURRENT_PARQUET, PRESENCE_PARQUET

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
        print(f"  ⚠️ Drive auth failed: {e}")
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


def upload_file_to_drive(service, local_path: str, drive_name: str, folder_id: str) -> bool:
    """
    Upload a single file to Drive, updating in-place if it already exists.
    Returns True on success.
    """
    try:
        existing = [
            f for f in list_drive_files(service, folder_id)
            if f['name'] == drive_name
        ]
        media = MediaFileUpload(local_path, mimetype='application/octet-stream', resumable=True)

        if existing:
            file_id = existing[0]['id']
            # Remove any duplicates beyond the first
            for dup in existing[1:]:
                service.files().delete(fileId=dup['id'], supportsAllDrives=True).execute()
                print(f"  🗑 Removed duplicate: {drive_name} (ID: {dup['id']})")
            service.files().update(
                fileId=file_id, media_body=media, supportsAllDrives=True
            ).execute()
            print(f"  ✓ Updated in-place: {drive_name} (file ID & link preserved ✅)")
        else:
            service.files().create(
                body={'name': drive_name, 'parents': [folder_id]},
                media_body=media, fields='id', supportsAllDrives=True,
            ).execute()
            print(f"  ✓ Created new: {drive_name} (first upload)")
            print(f"  ℹ️  Share this file's link now — it will never change.")
        return True
    except Exception as e:
        print(f"  ✗ Upload failed for {drive_name}: {e}")
        return False


def upload_all_to_drive(service, folder_id: str) -> int:
    """Upload both Parquet files to Drive. Returns count of successful uploads."""
    if not service or not folder_id:
        print("  ⚠️ Skipping upload (no service or folder ID)")
        return 0

    uploads = 0
    for local_path, drive_name in [
        (CURRENT_PARQUET,  'DGI_CURRENT.parquet'),
        (PRESENCE_PARQUET, 'DGI_PRESENCE.parquet'),
    ]:
        if os.path.exists(local_path):
            if upload_file_to_drive(service, local_path, drive_name, folder_id):
                uploads += 1
        else:
            print(f"  ⚠ Skipping {drive_name} — file not found locally")
    return uploads


def cleanup_old_files(service, folder_id, cutoff_date):
    if not service:
        return 0, 0
    deleted = kept = 0
    try:
        xlsx_mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        for f in list_drive_files(service, folder_id, mime_type=xlsx_mime):
            parsed = parse_filename_to_date(f['name'])
            if parsed:
                year, month = parsed
                if datetime(year, month, 1) >= cutoff_date:
                    kept += 1
                else:
                    service.files().delete(fileId=f['id'], supportsAllDrives=True).execute()
                    print(f"  🗑 Deleted old: {f['name']}")
                    deleted += 1
            else:
                kept += 1
    except Exception as e:
        print(f"  ⚠️ Cleanup error: {e}")
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
    log("DGI Cameroon — GitHub Actions Automation (CURRENT + PRESENCE)")
    log(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    log("=" * 70)

    DRIVE_FOLDER_ID = os.environ.get('DRIVE_FOLDER_ID', '')
    now             = datetime.now()
    cutoff_date     = datetime(now.year - YEARS_TO_KEEP, 1, 1)
    expected_year, expected_month = get_expected_latest_month()
    expected_month_name = FRENCH_MONTHS[expected_month]

    log(f"📅 Data window: {cutoff_date.strftime('%Y-%m')} → "
        f"{expected_year}-{expected_month:02d} (latest expected)")
    log(f"📄 Sentinel   : '{read_sentinel()}' → expected '{sentinel_month_key(expected_year, expected_month)}'")
    log("")
    log("📦 Output: 2 files")
    log("   DGI_CURRENT.parquet  — latest month snapshot  (~533k rows, ~40 MB)")
    log("   DGI_PRESENCE.parquet — NIU×YEAR×MONTH, all 62 months (~17M rows, ~60 MB)")

    # ── Sentinel check ────────────────────────────────────────────────────────
    if sentinel_already_downloaded():
        msg = (f"ℹ️  Sentinel confirms {expected_month_name} {expected_year} "
               f"already downloaded. Nothing to do.")
        log(msg)
        send_email_notification(
            subject=f"Skipped — {expected_month_name} {expected_year} already downloaded",
            body="\n".join(run_log), status="info",
        )
        sys.exit(0)

    # ── Probe DGI site ────────────────────────────────────────────────────────
    if not probe_latest_file_available():
        log(f"ℹ️  {expected_month_name} {expected_year} not yet on DGI site. "
            f"Will retry next scheduled run.")
        send_email_notification(
            subject=f"Skipped — {expected_month_name} {expected_year} not yet published",
            body="\n".join(run_log), status="warning",
        )
        sys.exit(0)

    log(f"✅ New file confirmed — proceeding with full pipeline.\n")

    # ── Download ──────────────────────────────────────────────────────────────
    months_to_process = get_month_list()
    log(f"📊 Will process {len(months_to_process)} months")
    downloaded, skipped, failed, failed_files = download_all_parallel(months_to_process)

    # ── Build Parquets ────────────────────────────────────────────────────────
    current_path, presence_path = build_parquets(newly_downloaded=downloaded)

    # ── Drive upload ──────────────────────────────────────────────────────────
    log("\n🔐 Authenticating Google Drive...")
    drive_service = authenticate_drive()

    uploaded = kept = deleted = 0
    if drive_service and DRIVE_FOLDER_ID:
        log("\n📤 Uploading to Google Drive (2 files)...")
        uploaded = upload_all_to_drive(drive_service, DRIVE_FOLDER_ID)
        log(f"   Uploaded: {uploaded} file(s)")

        log("\n🧹 Cleaning up Excel files older than 5 years...")
        kept, deleted = cleanup_old_files(drive_service, DRIVE_FOLDER_ID, cutoff_date)
        log(f"   Kept: {kept}, Deleted: {deleted}")
    else:
        log("\n⚠️ Skipping Drive operations (no credentials or folder ID)")

    # ── Sentinel ──────────────────────────────────────────────────────────────
    latest_filename = f"FICHIER_{expected_month_name}_{expected_year}.xlsx"
    if os.path.exists(os.path.join(DOWNLOAD_DIR, latest_filename)):
        write_sentinel(expected_year, expected_month)
        log(f"\n✅ Sentinel updated → '{sentinel_month_key(expected_year, expected_month)}'")
    else:
        log(f"\n⚠️  {latest_filename} not on disk — sentinel NOT updated. Will retry.")

    # ── Summary ───────────────────────────────────────────────────────────────
    end_time  = datetime.now()
    duration  = end_time - start_time
    cur_mb = (os.path.getsize(current_path)  / 1024 / 1024
              if current_path  and os.path.exists(current_path)  else 0)
    pre_mb = (os.path.getsize(presence_path) / 1024 / 1024
              if presence_path and os.path.exists(presence_path) else 0)

    summary_lines = [
        "",
        "=" * 70,
        "✅ EXECUTION COMPLETE",
        f"   Run duration         : {duration}",
        f"   Downloaded           : {downloaded} new files",
        f"   Skipped              : {skipped} (already existed)",
        f"   Failed               : {failed} (not found or error)",
        f"   DGI_CURRENT.parquet  : {cur_mb:.1f} MB",
        f"   DGI_PRESENCE.parquet : {pre_mb:.1f} MB",
        f"   Total in Power BI    : {cur_mb + pre_mb:.1f} MB  (was 660 MB)",
        f"   Drive upload         : {uploaded} file(s)",
        f"   Drive cleanup        : kept {kept}, deleted {deleted}",
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
        "success" if downloaded > 0      else
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