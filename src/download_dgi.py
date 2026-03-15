# =============================================================================
# DGI Cameroon - GitHub Actions Automation (OAuth Version)
# Rolling 5-Year Window: Always keep last 5 full years of data
#
# OUTPUT STRATEGY — 2 tables (Power BI optimised, ~80% size reduction):
#
#   DGI_TAXPAYERS.parquet  (~600k rows, ~25 MB)
#     └─ ONE row per unique NIU ever seen across all 59+ months.
#        Attributes (name, regime, centre…) reflect the LAST month the NIU
#        appeared — so historically-inactive taxpayers are fully searchable.
#        IS_ACTIVE = True  → present in the latest month.
#        IS_ACTIVE = False → was seen historically but absent from latest month.
#        Use for: company lookup, name search, ALL slicer dimensions.
#
#   DGI_PRESENCE.parquet   (~17M rows, ~60 MB)
#     └─ One row per (NIU_ID, YEAR, MONTH) — only 3 narrow int columns.
#        Powers ALL visuals: heatmap, trend charts, KPIs, new registrations.
#        Absent (NIU_ID, YEAR, MONTH) = taxpayer was inactive that month.
#
#   Relationship: DGI_TAXPAYERS[NIU_ID] → DGI_PRESENCE[NIU_ID] (1-to-many, both)
#   TOTAL: ~85 MB in Power BI  vs  660 MB previously.
#
# Build strategy:
#   Single chronological pass. Every month: read all columns, update a
#   rolling NIU→attrs dict with latest known attributes, accumulate presence
#   int arrays. At end: write TAXPAYERS from the dict, PRESENCE from arrays.
#   Peak RAM ≈ one month DataFrame (~120 MB) + rolling dict (~80 MB).
# =============================================================================

import os
import sys
import time
import smtplib
import requests
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import random

# =============================================================================
# CONFIGURATION
# =============================================================================
YEARS_TO_KEEP    = 5
DOWNLOAD_WORKERS = 5
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

# Complete taxpayer dimension — one row per unique NIU ever seen
# IS_ACTIVE = True  → present in the latest downloaded month
# IS_ACTIVE = False → seen historically but absent from latest month (still searchable!)
TAXPAYERS_SCHEMA = pa.schema([
    pa.field('NIU_ID',                 pa.int32()),
    pa.field('NIU',                    pa.string()),
    pa.field('RAISON_SOCIALE',         pa.string()),
    pa.field('SIGLE',                  pa.string()),
    pa.field('ACTIVITE_PRINCIPALE',    pa.string()),
    pa.field('REGIME',                 pa.string()),
    pa.field('CRI',                    pa.string()),
    pa.field('CENTRE_DE_RATTACHEMENT', pa.string()),
    pa.field('LAST_SEEN_YEAR',         pa.int16()),
    pa.field('LAST_SEEN_MONTH',        pa.int16()),
    pa.field('IS_ACTIVE',              pa.bool_()),
])

# Per-taxpayer monthly presence — 3 narrow int columns only
# Relationship: DGI_TAXPAYERS[NIU_ID] → DGI_PRESENCE[NIU_ID]  (1-to-many, both)
PRESENCE_SCHEMA = pa.schema([
    pa.field('NIU_ID', pa.int32()),
    pa.field('YEAR',   pa.int16()),
    pa.field('MONTH',  pa.int16()),
])

BASE_HOST         = "https://teledeclaration-dgi.cm/UploadedFiles/AttachedFiles/ArchiveListecontribuable"
DOWNLOAD_DIR      = "/tmp/dgi_downloads"
TAXPAYERS_PARQUET = "/tmp/dgi_downloads/DGI_TAXPAYERS.parquet"
PRESENCE_PARQUET  = "/tmp/dgi_downloads/DGI_PRESENCE.parquet"
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

# =============================================================================
# BUILD TAXPAYERS + PRESENCE PARQUETS
# =============================================================================

def build_parquets(newly_downloaded: int) -> tuple:
    """
    Build both Power BI output files in one chronological pass.

    DGI_TAXPAYERS.parquet  (~600k rows, ~25 MB)
      One row per unique NIU ever seen across all months.
      Attributes = most recent known values (last month the NIU appeared).
      IS_ACTIVE = True  → present in the latest month.
      IS_ACTIVE = False → historically present but absent from latest month.
      This means every taxpayer ever registered is searchable in Power BI,
      not just those active right now.

    DGI_PRESENCE.parquet   (~17M rows, ~60 MB)
      One row per (NIU_ID, YEAR, MONTH). Pure int32/int16 — no strings.

    Memory strategy
    ───────────────
    All months: read all canonical columns (needed to keep attrs fresh).
    Rolling dict  NIU → (attrs tuple + last_year + last_month): ~80 MB peak.
    Presence accumulators (int arrays): ~135 MB for 17M rows.
    One month DataFrame in RAM at a time: ~120 MB.
    Peak total: ~335 MB — well within GitHub Actions 7 GB.
    """
    both_exist = (
        os.path.exists(TAXPAYERS_PARQUET) and
        os.path.exists(PRESENCE_PARQUET)
    )
    if newly_downloaded == 0 and both_exist:
        tax_mb = os.path.getsize(TAXPAYERS_PARQUET) / 1024 / 1024
        pre_mb = os.path.getsize(PRESENCE_PARQUET)  / 1024 / 1024
        print(f"\n📊 No new files — reusing existing Parquets "
              f"(Taxpayers: {tax_mb:.1f} MB, Presence: {pre_mb:.1f} MB)")
        return TAXPAYERS_PARQUET, PRESENCE_PARQUET

    print("\n📊 Building TAXPAYERS + PRESENCE (single chronological pass)...")

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

    # ── State ─────────────────────────────────────────────────────────────────
    niu_to_id: dict  = {}   # NIU string → int32 ID (assigned on first encounter)

    # last_seen[NIU] = (RAISON_SOCIALE, SIGLE, ACTIVITE_PRINCIPALE,
    #                   REGIME, CRI, CENTRE_DE_RATTACHEMENT,
    #                   last_year, last_month)
    last_seen: dict  = {}

    latest_niu_set: set = set()   # NIUs present in the latest month

    # ── Presence accumulators ─────────────────────────────────────────────────
    acc_ids    = []
    acc_years  = []
    acc_months = []
    total_rows = 0

    ATTR_COLS = ['RAISON_SOCIALE', 'SIGLE', 'ACTIVITE_PRINCIPALE',
                 'REGIME', 'CRI', 'CENTRE_DE_RATTACHEMENT']

    for i, (year, month, filename) in enumerate(dated_files, 1):
        filepath  = os.path.join(DOWNLOAD_DIR, filename)
        is_latest = (year == latest_year and month == latest_month)
        try:
            raw = pd.read_excel(filepath, sheet_name=0, dtype=str, engine=excel_engine)
            raw.dropna(how='all', inplace=True)
            df  = normalize_df(raw)
            del raw
        except Exception as e:
            print(f"  [{i}/{total}] ✗ Failed to read {filename}: {e}")
            continue

        df_valid = df[df['NIU'] != ''].copy()
        del df

        # ── Assign IDs to any new NIUs encountered this month ─────────────────
        s_nius   = df_valid['NIU']
        new_mask = ~s_nius.isin(niu_to_id)
        new_nius = s_nius[new_mask].unique()
        start    = len(niu_to_id)
        niu_to_id.update(zip(new_nius, range(start, start + len(new_nius))))
        ids = s_nius.map(niu_to_id).to_numpy(dtype=np.int32)

        # ── Update rolling last_seen dict ─────────────────────────────────────
        # Iterating row-by-row is slow for 500k rows; use vectorised approach.
        # Build a dict from this month's data then merge into last_seen.
        # Since we iterate chronologically, simply overwrite — later = newer.
        month_attrs = dict(zip(
            df_valid['NIU'],
            zip(*(df_valid[c] for c in ATTR_COLS),
                [year]  * len(df_valid),
                [month] * len(df_valid))
        ))
        last_seen.update(month_attrs)

        if is_latest:
            latest_niu_set = set(df_valid['NIU'])

        # ── Accumulate presence ───────────────────────────────────────────────
        n = len(ids)
        acc_ids.append(ids)
        acc_years.append(np.full(n, year,  dtype=np.int16))
        acc_months.append(np.full(n, month, dtype=np.int16))
        total_rows += n

        tag = '→ TAXPAYERS + PRESENCE' if is_latest else '→ PRESENCE'
        print(f"  [{i}/{total}] ✓ {filename} — {n:,} rows {tag}")

        del df_valid, s_nius, ids, month_attrs

    # ── Write DGI_TAXPAYERS ───────────────────────────────────────────────────
    print(f"\n  📝 Building taxpayer dimension from {len(last_seen):,} unique NIUs...")

    niu_list   = list(last_seen.keys())
    attrs_list = list(last_seen.values())

    taxpayers_df = pd.DataFrame({
        'NIU_ID':                 pd.array([niu_to_id[n] for n in niu_list], dtype='int32'),
        'NIU':                    niu_list,
        'RAISON_SOCIALE':         [a[0] for a in attrs_list],
        'SIGLE':                  [a[1] for a in attrs_list],
        'ACTIVITE_PRINCIPALE':    [a[2] for a in attrs_list],
        'REGIME':                 [a[3] for a in attrs_list],
        'CRI':                    [a[4] for a in attrs_list],
        'CENTRE_DE_RATTACHEMENT': [a[5] for a in attrs_list],
        'LAST_SEEN_YEAR':         pd.array([a[6] for a in attrs_list], dtype='int16'),
        'LAST_SEEN_MONTH':        pd.array([a[7] for a in attrs_list], dtype='int16'),
        'IS_ACTIVE':              [n in latest_niu_set for n in niu_list],
    })
    del last_seen, niu_list, attrs_list

    active_count   = taxpayers_df['IS_ACTIVE'].sum()
    inactive_count = len(taxpayers_df) - active_count

    pq.write_table(
        pa.Table.from_pandas(taxpayers_df, schema=TAXPAYERS_SCHEMA, preserve_index=False),
        TAXPAYERS_PARQUET,
        compression='snappy', use_dictionary=True, write_statistics=True,
    )
    del taxpayers_df

    # ── Write DGI_PRESENCE ────────────────────────────────────────────────────
    all_ids    = np.concatenate(acc_ids)
    all_years  = np.concatenate(acc_years)
    all_months = np.concatenate(acc_months)
    del acc_ids, acc_years, acc_months

    pq.write_table(
        pa.table({
            'NIU_ID': pa.array(all_ids,    type=pa.int32()),
            'YEAR':   pa.array(all_years,  type=pa.int16()),
            'MONTH':  pa.array(all_months, type=pa.int16()),
        }),
        PRESENCE_PARQUET,
        compression='snappy', write_statistics=True,
    )
    del all_ids, all_years, all_months

    tax_mb = os.path.getsize(TAXPAYERS_PARQUET) / 1024 / 1024
    pre_mb = os.path.getsize(PRESENCE_PARQUET)  / 1024 / 1024

    print(f"\n  ✅ TAXPAYERS : {tax_mb:.1f} MB  "
          f"({active_count:,} active + {inactive_count:,} historical = {active_count+inactive_count:,} total)")
    print(f"  ✅ PRESENCE  : {pre_mb:.1f} MB  ({total_rows:,} rows)")
    print(f"\n  📉 Total in Power BI: {tax_mb + pre_mb:.1f} MB  (was 660 MB)")

    return TAXPAYERS_PARQUET, PRESENCE_PARQUET

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
    log("DGI Cameroon — GitHub Actions Automation (TAXPAYERS + PRESENCE)")
    log(f"Started: {start_time.strftime('%Y-%m-%d %H:%M:%S UTC')}")
    log("=" * 70)

    now          = datetime.now()
    expected_year, expected_month = get_expected_latest_month()
    expected_month_name = FRENCH_MONTHS[expected_month]

    log(f"📅 Data window: {now.year - YEARS_TO_KEEP}-01 → "
        f"{expected_year}-{expected_month:02d} (latest expected)")
    log(f"📄 Sentinel   : '{read_sentinel()}' → expected '{sentinel_month_key(expected_year, expected_month)}'")
    log("")
    log("📦 Output: 2 files → GitHub Release assets")
    log("   DGI_TAXPAYERS.parquet — all unique NIUs ever seen (~600k rows, ~25 MB)")
    log("   DGI_PRESENCE.parquet  — NIU_ID×YEAR×MONTH, all months (~17M rows, ~60 MB)")

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
    taxpayers_path, presence_path = build_parquets(newly_downloaded=downloaded)

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
    tax_mb    = os.path.getsize(taxpayers_path) / 1024 / 1024 if taxpayers_path and os.path.exists(taxpayers_path) else 0
    pre_mb    = os.path.getsize(presence_path)  / 1024 / 1024 if presence_path  and os.path.exists(presence_path)  else 0

    summary_lines = [
        "",
        "=" * 70,
        "✅ EXECUTION COMPLETE",
        f"   Run duration            : {duration}",
        f"   Downloaded              : {downloaded} new files",
        f"   Skipped                 : {skipped} (already existed)",
        f"   Failed                  : {failed} (not found or error)",
        f"   DGI_TAXPAYERS.parquet   : {tax_mb:.1f} MB",
        f"   DGI_PRESENCE.parquet    : {pre_mb:.1f} MB",
        f"   Total in Power BI       : {tax_mb + pre_mb:.1f} MB  (was 660 MB)",
        f"   Upload                  : handled by YAML (gh release upload)",
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