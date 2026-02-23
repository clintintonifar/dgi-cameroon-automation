# =============================================================================
# DGI Cameroon - GitHub Actions Automation (OAuth Version)
# Rolling 5-Year Window: Always keep last 60 months of data
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
# Naming-convention fix (v2):
#   The DGI site uses inconsistent separators between FICHIER / MONTH / YEAR.
#   build_candidate_urls() tries all 8 variants (4 separator combos × 2 cases)
#   so files like FICHIER_MARS 2025.xlsx or FICHIER JUIN_2023.xlsx are found.
# =============================================================================

import os
import time
import requests
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
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
MONTHS_TO_DOWNLOAD = YEARS_TO_KEEP * 12
DOWNLOAD_WORKERS   = 3     # Optimal for GitHub Actions 2-core free runners
MAX_RETRIES        = 5
RETRY_DELAY        = 5
REQUEST_TIMEOUT    = 20    # Faster fail-and-retry instead of 60s hang

FRENCH_MONTHS = {
    1: 'JANVIER', 2: 'FEVRIER', 3: 'MARS', 4: 'AVRIL',
    5: 'MAI', 6: 'JUIN', 7: 'JUILLET', 8: 'AOUT',
    9: 'SEPTEMBRE', 10: 'OCTOBRE', 11: 'NOVEMBRE', 12: 'DECEMBRE'
}

# Canonical columns present in both old and new file formats.
# Old files had ~21 cols — legacy-only ones are silently dropped.
CANONICAL_COLUMNS = [
    'RAISON_SOCIALE',
    'SIGLE',
    'NIU',
    'ACTIVITE_PRINCIPALE',
    'REGIME',
    'CRI',
    'CENTRE_DE_RATTACHEMENT',
]

# PyArrow schema — enforces types at write time, makes Parquet tighter
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

# Base URL — no separator baked in; build_candidate_urls() handles all variants
BASE_HOST        = "https://teledeclaration-dgi.cm/UploadedFiles/AttachedFiles/ArchiveListecontribuable"
DOWNLOAD_DIR     = "/tmp/dgi_downloads"
COMBINED_PARQUET = "/tmp/dgi_downloads/DGI_COMBINED.parquet"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

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


def get_month_list():
    """Generate list of (year, month) tuples for the last 60 months."""
    months = set()
    current = datetime.now().replace(day=1)
    for i in range(MONTHS_TO_DOWNLOAD):
        target = current - timedelta(days=30 * i)
        if target <= datetime.now():
            months.add((target.year, target.month))
    return list(months)

# =============================================================================
# URL CANDIDATE BUILDER — handles all known naming conventions
# =============================================================================

def build_candidate_urls(month_name: str, year: int) -> list:
    """
    Return every plausible URL for one month's file.

    The DGI site has used at least four naming conventions over the years:
      • FICHIER JUIN 2025.xlsx   (space  / space)   ← original / most common
      • FICHIER_JUIN 2025.xlsx   (under  / space)
      • FICHIER JUIN_2025.xlsx   (space  / under)
      • FICHIER_JUIN_2025.xlsx   (under  / under)

    We also try both upper-case and title-case month names because a handful
    of files have been spotted as "Juin" instead of "JUIN".

    Tried in order of likelihood so the most common pattern hits first,
    minimising unnecessary HTTP round-trips on the happy path.
    """
    separators = [
        ('%20', '%20'),   # "FICHIER JUIN 2025"  ← most common
        ('_',   '%20'),   # "FICHIER_JUIN 2025"
        ('%20', '_'),     # "FICHIER JUIN_2025"
        ('_',   '_'),     # "FICHIER_JUIN_2025"
    ]

    month_variants = [
        month_name,              # "JUIN"
        month_name.capitalize(), # "Juin"
    ]

    urls = []
    for mv in month_variants:
        for s1, s2 in separators:
            filename = f"FICHIER{s1}{mv}{s2}{year}.xlsx"
            urls.append(f"{BASE_HOST}/{filename}")

    return urls

# =============================================================================
# DOWNLOAD — PARALLEL WITH FAST TIMEOUT + RETRY
# =============================================================================

def download_file(year, month):
    """
    Download one month's file, trying every naming-convention variant.

    Returns (year, month, status) where status is one of:
        'skipped'    — file already on disk, nothing to do
        'downloaded' — fetched successfully from one of the URL variants
        'not_found'  — all 8 URL patterns returned 404 after all retries
        'failed'     — network / server error on every attempt

    Two-level loop design:
        Outer loop — iterates URL variants; breaks immediately on 404
                     (no point retrying a URL that literally doesn't exist)
        Inner loop — retries only for genuine network/server errors (5xx, timeouts)
    """
    month_name = FRENCH_MONTHS[month]
    filename   = f"FICHIER_{month_name}_{year}.xlsx"   # canonical local name
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
                    # Save under canonical local name regardless of which URL
                    # variant succeeded — keeps the rest of the pipeline simple.
                    with open(filepath, 'wb') as f:
                        f.write(response.content)
                    return year, month, 'downloaded'

                elif response.status_code == 404:
                    # This URL variant doesn't exist — no point retrying it.
                    last_status = 'not_found'
                    break   # break inner retry loop → try next URL variant

                else:
                    # Unexpected server error — worth retrying same URL.
                    last_status = 'failed'
                    if attempt < MAX_RETRIES - 1:
                        time.sleep(RETRY_DELAY + random.uniform(1, 3))

            except Exception:
                last_status = 'failed'
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY + random.uniform(1, 3))

    return year, month, last_status


def download_all_parallel(months_to_process):
    """
    Download all months using a thread pool.
    DOWNLOAD_WORKERS=3 is optimal for GitHub Actions free tier (2 cores).
    More threads cause context-switching overhead and net slower performance.
    """
    downloaded = skipped = failed = 0
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
                print(f"{label} ⚠ Not found: FICHIER_{month_name}_{year}.xlsx")
            else:
                failed += 1
                print(f"{label} ✗ Failed: FICHIER_{month_name}_{year}.xlsx")

    return downloaded, skipped, failed

# =============================================================================
# COLUMN NORMALIZATION
# =============================================================================

def normalize_to_arrow_table(df, year, month):
    """
    Normalize a raw Excel DataFrame and convert directly to a PyArrow Table.
    Returning an Arrow Table instead of a pandas DataFrame avoids a redundant
    pandas→arrow conversion inside the ParquetWriter, saving time and memory.
    """
    # Normalize column names
    df.columns = [str(c).strip().upper() for c in df.columns]

    # Drop legacy row-number index columns from old format
    for drop_col in ['N°', 'N', 'Nº']:
        if drop_col in df.columns:
            df.drop(columns=[drop_col], inplace=True)

    # Add any missing canonical columns as empty strings
    for col in CANONICAL_COLUMNS:
        if col not in df.columns:
            df[col] = ''

    df = df[CANONICAL_COLUMNS].copy()

    # Clean string values
    for col in CANONICAL_COLUMNS:
        df[col] = df[col].astype(str).str.strip().replace('nan', '')

    # Inject period columns
    n = len(df)
    df.insert(0, 'MONTH', pd.array([month] * n, dtype='int16'))
    df.insert(0, 'YEAR',  pd.array([year]  * n, dtype='int16'))

    # Convert directly to PyArrow Table with enforced schema
    return pa.Table.from_pandas(df, schema=PARQUET_SCHEMA, preserve_index=False)

# =============================================================================
# COMBINE — INCREMENTAL PARQUET STREAMING (NO MEMORY SPIKE)
# =============================================================================

def combine_to_parquet(newly_downloaded):
    """
    Stream each Excel file through normalization and write it immediately
    to Parquet using PyArrow's ParquetWriter.

    Pattern:
        Excel → normalize → Arrow Table → ParquetWriter.write_table()
                                          (appends to same file, row group by row group)

    This replaces the old pattern:
        Excel → DataFrame → all_frames.append() → pd.concat() → to_parquet()
    which held ALL data in RAM before writing anything.

    The new pattern holds only ONE file in memory at a time — safe at any scale.
    Skips full rebuild when no new files were downloaded.
    """
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

    # Prefer calamine (Rust, 3-6x faster), fall back to openpyxl
    try:
        import python_calamine  # noqa: F401
        excel_engine = 'calamine'
        print("  ⚡ Excel engine: calamine (Rust-based)")
    except ImportError:
        excel_engine = 'openpyxl'
        print("  ⚠️ Excel engine: openpyxl (install python-calamine for 3-6x speedup)")

    total_rows = 0

    # ParquetWriter opens the file once and appends one Arrow Table per Excel file.
    # snappy compression: best balance of speed vs size for this workload.
    # row_group_size=100_000: each Excel file becomes one row group — efficient
    # for Power BI which can skip row groups during predicate pushdown.
    with pq.ParquetWriter(
        COMBINED_PARQUET,
        schema=PARQUET_SCHEMA,
        compression='snappy',
        use_dictionary=True,       # Dictionary-encodes repetitive strings (REGIME, CRI, etc.)
        write_statistics=True,     # Enables min/max stats per column — speeds up Power BI filters
    ) as writer:

        for i, filename in enumerate(xlsx_files, 1):
            parsed = parse_filename_to_date(filename)
            if parsed is None:
                print(f"  ⚠ Skipping (unparseable name): {filename}")
                continue

            year, month = parsed
            filepath = os.path.join(DOWNLOAD_DIR, filename)

            try:
                # Read Excel — calamine is 3-6x faster than openpyxl here
                df = pd.read_excel(
                    filepath,
                    sheet_name=0,
                    dtype=str,
                    engine=excel_engine,
                )
                df.dropna(how='all', inplace=True)

                # Normalize + convert to Arrow Table in one step
                arrow_table = normalize_to_arrow_table(df, year, month)

                # Write this file's rows immediately — no accumulation in RAM
                writer.write_table(arrow_table, row_group_size=100_000)

                total_rows += len(arrow_table)
                print(f"  [{i}/{len(xlsx_files)}] ✓ {filename} — {len(arrow_table):,} rows")

            except Exception as e:
                print(f"  [{i}/{len(xlsx_files)}] ✗ Failed: {filename} — {str(e)}")
                continue

    if total_rows == 0:
        print("  ✗ No data written.")
        os.remove(COMBINED_PARQUET)
        return None

    size_mb = os.path.getsize(COMBINED_PARQUET) / 1024 / 1024
    print(f"\n  ✅ Parquet ready: {total_rows:,} rows, {size_mb:.1f} MB")

    return COMBINED_PARQUET

# =============================================================================
# GOOGLE DRIVE AUTHENTICATION
# =============================================================================

def authenticate_drive():
    """Authenticate using OAuth Refresh Token."""
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
            None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=['https://www.googleapis.com/auth/drive'],
        )

        service = build('drive', 'v3', credentials=creds)
        print("  ✅ Google Drive authenticated via OAuth")
        return service

    except Exception as e:
        print(f"  ⚠️ Drive auth failed: {str(e)}")
        return None

# =============================================================================
# DRIVE — LIST FILES WITH PAGINATION (safe for large folders)
# =============================================================================

def list_drive_files(service, folder_id, mime_type=None):
    """
    List all files in a Drive folder, following pagination tokens.
    Without pagination, files().list() only returns the first page (~100 files).
    As the folder grows past 100 files, old files would silently escape cleanup.
    """
    query = f"'{folder_id}' in parents"
    if mime_type:
        query += f" and mimeType='{mime_type}'"

    files = []
    page_token = None

    while True:
        kwargs = {
            'q':               query,
            'fields':          'nextPageToken, files(id, name)',
            'supportsAllDrives': True,
            'pageSize':        1000,   # Max allowed per page
        }
        if page_token:
            kwargs['pageToken'] = page_token

        result     = service.files().list(**kwargs).execute()
        files     += result.get('files', [])
        page_token = result.get('nextPageToken')

        if not page_token:
            break

    return files

# =============================================================================
# UPLOAD TO GOOGLE DRIVE — IN-PLACE UPDATE (PRESERVES FILE ID & PUBLIC LINK)
# =============================================================================

def upload_to_drive(service, drive_folder_id):
    """
    Upload the combined Parquet file to Google Drive.

    Strategy:
      - If the file already exists in Drive → use files().update() to replace
        its content IN-PLACE. The file ID stays the same, so any public sharing
        link shared previously will continue to work forever.
      - If no file exists yet (first run) → use files().create() to create it.
        After this first upload, share the link — it will never change again.

    This replaces the old delete + create pattern which generated a new file ID
    (and therefore a new link) on every single run.
    """
    if not service:
        print("  ⚠️ No Drive service — skipping upload")
        return 0

    if not os.path.exists(COMBINED_PARQUET):
        print("  ⚠️ Parquet file not found — skipping upload")
        return 0

    uploaded = 0
    parquet_name = "DGI_COMBINED.parquet"

    try:
        # Find all files in the folder matching our target name
        existing_files = [
            f for f in list_drive_files(service, drive_folder_id)
            if f['name'] == parquet_name
        ]

        media = MediaFileUpload(
            COMBINED_PARQUET,
            mimetype='application/octet-stream',
            resumable=True,   # Avoids silent timeout failures for large files
        )

        if existing_files:
            # ✅ FILE ALREADY EXISTS — update content in-place
            # files().update() replaces the bytes but keeps the same file ID,
            # so all previously shared public links remain valid.
            existing_id = existing_files[0]['id']

            # If duplicates somehow exist, clean them up first
            for duplicate in existing_files[1:]:
                service.files().delete(
                    fileId=duplicate['id'],
                    supportsAllDrives=True,
                ).execute()
                print(f"  🗑 Removed duplicate copy (ID: {duplicate['id']})")

            service.files().update(
                fileId=existing_id,
                media_body=media,
                supportsAllDrives=True,
            ).execute()
            print(f"  ✓ Updated in-place: {parquet_name} (file ID & public link preserved ✅)")

        else:
            # 🆕 FIRST RUN — file does not exist yet, create it
            service.files().create(
                body={'name': parquet_name, 'parents': [drive_folder_id]},
                media_body=media,
                fields='id',
                supportsAllDrives=True,
            ).execute()
            print(f"  ✓ Created new: {parquet_name} (first upload)")
            print(f"  ℹ️  Share this file's public link now — it will never change on future runs.")

        uploaded = 1

    except Exception as e:
        print(f"  ✗ Upload failed for Parquet: {str(e)}")

    return uploaded

# =============================================================================
# CLEANUP OLD FILES
# =============================================================================

def cleanup_old_files(service, drive_folder_id, cutoff_date):
    """
    Delete Excel files older than 5 years from Google Drive.
    Uses paginated list_drive_files() — safe as folder grows past 100 files.
    """
    if not service:
        print("  ⚠️ No Drive service — skipping cleanup")
        return 0, 0

    deleted = kept = 0

    try:
        xlsx_mime = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        all_files = list_drive_files(service, drive_folder_id, mime_type=xlsx_mime)

        for file in all_files:
            filename = file['name']
            file_id  = file['id']
            parsed   = parse_filename_to_date(filename)

            if parsed:
                year, month = parsed
                file_date = datetime(year, month, 1)
                if file_date >= cutoff_date:
                    kept += 1
                else:
                    service.files().delete(fileId=file_id, supportsAllDrives=True).execute()
                    print(f"  🗑 Deleted old: {filename}")
                    deleted += 1
            else:
                kept += 1   # Keep unparseable files (safety)

    except Exception as e:
        print(f"  ⚠️ Cleanup error: {str(e)}")

    return kept, deleted

# =============================================================================
# MAIN
# =============================================================================

def main():
    print("=" * 70)
    print("DGI Cameroon - GitHub Actions Automation (OAuth)")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    DRIVE_FOLDER_ID = os.environ.get('DRIVE_FOLDER_ID', '')

    cutoff_date = datetime.now() - timedelta(days=YEARS_TO_KEEP * 365)
    print(f"📅 Data window: {cutoff_date.strftime('%Y-%m')} → {datetime.now().strftime('%Y-%m')}")

    months_to_process = get_month_list()
    print(f"📊 Will process {len(months_to_process)} months")

    # Step 1: Parallel download
    downloaded, skipped, failed = download_all_parallel(months_to_process)

    # Step 2: Incremental Parquet build (skips if nothing new)
    parquet_path = combine_to_parquet(newly_downloaded=downloaded)

    # Step 3: Auth
    print("\n🔐 Authenticating Google Drive...")
    drive_service = authenticate_drive()

    # Step 4: Upload + cleanup
    if drive_service and DRIVE_FOLDER_ID:
        print("\n📤 Uploading to Google Drive...")
        uploaded = upload_to_drive(drive_service, DRIVE_FOLDER_ID)
        print(f"   Uploaded: {uploaded} files")

        print("\n🧹 Cleaning up files older than 5 years...")
        kept, deleted = cleanup_old_files(drive_service, DRIVE_FOLDER_ID, cutoff_date)
        print(f"   Kept: {kept}, Deleted: {deleted}")
    else:
        print("\n⚠️ Skipping Drive operations (no credentials or folder ID)")

    # Summary
    print("\n" + "=" * 70)
    print("✅ EXECUTION COMPLETE")
    print(f"   Downloaded: {downloaded} new files")
    print(f"   Skipped:    {skipped} (already existed)")
    print(f"   Failed:     {failed} (not found or error)")
    if parquet_path and os.path.exists(parquet_path):
        size_mb = os.path.getsize(parquet_path) / 1024 / 1024
        print(f"   Parquet:    {size_mb:.1f} MB → {parquet_path}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)


if __name__ == "__main__":
    main()
