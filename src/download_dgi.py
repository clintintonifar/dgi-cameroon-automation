# =============================================================================
# DGI Cameroon - GitHub Actions Automation (OAuth Version)
# Rolling 5-Year Window: Always keep last 60 months of data
# Uses OAuth Refresh Token (Personal Account Quota)
# ALL credentials loaded from GitHub Secrets (environment variables)
# =============================================================================

import os
import time
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
import random

# =============================================================================
# CONFIGURATION
# =============================================================================
YEARS_TO_KEEP = 5
MONTHS_TO_DOWNLOAD = YEARS_TO_KEEP * 12
DOWNLOAD_WORKERS = 6       # Parallel download threads
MAX_RETRIES = 5
RETRY_DELAY = 5

FRENCH_MONTHS = {
    1: 'JANVIER', 2: 'FEVRIER', 3: 'MARS', 4: 'AVRIL',
    5: 'MAI', 6: 'JUIN', 7: 'JUILLET', 8: 'AOUT',
    9: 'SEPTEMBRE', 10: 'OCTOBRE', 11: 'NOVEMBRE', 12: 'DECEMBRE'
}

# Canonical columns present in both old and new file formats
CANONICAL_COLUMNS = [
    'RAISON_SOCIALE',
    'SIGLE',
    'NIU',
    'ACTIVITE_PRINCIPALE',
    'REGIME',
    'CRI',
    'CENTRE_DE_RATTACHEMENT',
]

BASE_URL = "https://teledeclaration-dgi.cm/UploadedFiles/AttachedFiles/ArchiveListecontribuable/FICHIER%20{}%20{}.xlsx"
DOWNLOAD_DIR = "/tmp/dgi_downloads"
COMBINED_CSV = "/tmp/dgi_downloads/DGI_COMBINED.csv"
os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def parse_filename_to_date(filename):
    """Extract year/month from filename like 'FICHIER_JANVIER_2026.xlsx'"""
    try:
        clean = filename.replace("FICHIER_", "").replace(".xlsx", "")
        parts = clean.split("_")
        if len(parts) != 2:
            return None
        month_name, year_str = parts
        year = int(year_str)
        month_num = None
        for num, name in FRENCH_MONTHS.items():
            if name == month_name:
                month_num = num
                break
        return (year, month_num) if month_num else None
    except:
        return None

def get_month_list():
    """Generate list of (year, month) for last 60 months"""
    months = []
    current = datetime.now().replace(day=1)
    for i in range(MONTHS_TO_DOWNLOAD):
        target = current - timedelta(days=30*i)
        if target <= datetime.now():
            months.append((target.year, target.month))
    return list(set(months))

# =============================================================================
# DOWNLOAD FUNCTION (With Retry Logic)
# =============================================================================

def download_file(year, month):
    """
    Download single month's file with retry logic.
    Returns (year, month, status) where status is:
      'skipped'    — file already exists locally
      'downloaded' — successfully fetched
      'not_found'  — 404 after all retries
      'failed'     — other error
    """
    month_name = FRENCH_MONTHS[month]
    url = BASE_URL.format(month_name, year)
    filename = f"FICHIER_{month_name}_{year}.xlsx"
    filepath = os.path.join(DOWNLOAD_DIR, filename)

    if os.path.exists(filepath):
        return year, month, 'skipped'

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, application/octet-stream, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Connection": "keep-alive",
        "Cache-Control": "no-cache"
    }

    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers, timeout=60)

            if response.status_code == 200:
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                return year, month, 'downloaded'

            elif response.status_code == 404:
                if attempt < MAX_RETRIES - 1:
                    time.sleep(RETRY_DELAY + random.uniform(1, 3))
                else:
                    return year, month, 'not_found'
            else:
                return year, month, 'failed'

        except Exception:
            if attempt < MAX_RETRIES - 1:
                time.sleep(RETRY_DELAY + random.uniform(1, 3))
            else:
                return year, month, 'failed'

    return year, month, 'failed'


def download_all_parallel(months_to_process):
    """
    Download all months in parallel using a thread pool.
    Returns counts: downloaded, skipped, failed.
    """
    downloaded = 0
    skipped = 0
    failed = 0
    total = len(months_to_process)

    print(f"\n📥 Downloading {total} months ({DOWNLOAD_WORKERS} threads in parallel)...")

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

            if status == 'downloaded':
                downloaded += 1
                print(f"  [{completed}/{total}] ✓ Downloaded: FICHIER_{month_name}_{year}.xlsx")
            elif status == 'skipped':
                skipped += 1
                print(f"  [{completed}/{total}] → Skip: FICHIER_{month_name}_{year}.xlsx (exists)")
            elif status == 'not_found':
                failed += 1
                print(f"  [{completed}/{total}] ⚠ Not found: FICHIER_{month_name}_{year}.xlsx")
            else:
                failed += 1
                print(f"  [{completed}/{total}] ✗ Failed: FICHIER_{month_name}_{year}.xlsx")

    return downloaded, skipped, failed

# =============================================================================
# COLUMN NORMALIZATION
# =============================================================================

def normalize_columns(df, year, month):
    """
    Normalize column names, keep only canonical columns,
    fill missing canonical columns with empty string,
    and inject YEAR + MONTH.
    """
    df.columns = [str(c).strip().upper() for c in df.columns]

    # Drop row-number index columns from old format
    for drop_col in ['N°', 'N', 'Nº']:
        if drop_col in df.columns:
            df = df.drop(columns=[drop_col])

    # Add missing canonical columns as empty
    for col in CANONICAL_COLUMNS:
        if col not in df.columns:
            df[col] = ''

    df = df[CANONICAL_COLUMNS].copy()

    # Strip whitespace from all values
    for col in CANONICAL_COLUMNS:
        df[col] = df[col].astype(str).str.strip().replace('nan', '')

    # Inject source period at the front
    df.insert(0, 'MONTH', month)
    df.insert(0, 'YEAR', year)

    return df

# =============================================================================
# COMBINE ALL EXCEL FILES INTO ONE CSV (Streaming + Calamine Engine)
# =============================================================================

def combine_to_csv(newly_downloaded):
    """
    Stream-write all Excel files into a single CSV one file at a time.
    Uses python-calamine (Rust-based) for 3-6x faster Excel reading vs openpyxl.
    Skips full rebuild if no new files were downloaded and CSV already exists.
    Returns the path to the CSV or None on failure.
    """
    # Skip rebuild if nothing new was downloaded and CSV already exists
    if newly_downloaded == 0 and os.path.exists(COMBINED_CSV):
        size_mb = os.path.getsize(COMBINED_CSV) / 1024 / 1024
        print(f"\n📊 No new files — reusing existing combined CSV ({size_mb:.1f} MB)")
        return COMBINED_CSV

    print("\n📊 Combining Excel files into single CSV (streaming, calamine engine)...")

    xlsx_files = sorted([
        f for f in os.listdir(DOWNLOAD_DIR)
        if f.endswith('.xlsx') and f.startswith('FICHIER_')
    ])

    if not xlsx_files:
        print("  ⚠️ No Excel files found to combine.")
        return None

    # Detect available Excel engine: prefer calamine (fast, Rust-based),
    # fall back to openpyxl if not installed.
    try:
        import python_calamine  # noqa: F401
        excel_engine = 'calamine'
        print("  ⚡ Using calamine engine (fast Rust-based reader)")
    except ImportError:
        excel_engine = 'openpyxl'
        print("  ⚠️ calamine not found, falling back to openpyxl (install python-calamine for 3-6x speedup)")

    total_rows = 0
    first_file = True

    with open(COMBINED_CSV, 'w', encoding='utf-8-sig', newline='') as out:
        for i, filename in enumerate(xlsx_files, 1):
            parsed = parse_filename_to_date(filename)
            if parsed is None:
                print(f"  ⚠ Skipping (unparseable name): {filename}")
                continue

            year, month = parsed
            filepath = os.path.join(DOWNLOAD_DIR, filename)

            try:
                df = pd.read_excel(
                    filepath,
                    sheet_name=0,
                    dtype=str,
                    engine=excel_engine
                )

                df = df.dropna(how='all')
                df = normalize_columns(df, year, month)

                # Stream write — header only on first file
                df.to_csv(out, index=False, header=first_file)
                first_file = False

                total_rows += len(df)
                print(f"  [{i}/{len(xlsx_files)}] ✓ {filename} — {len(df):,} rows")

            except Exception as e:
                print(f"  [{i}/{len(xlsx_files)}] ✗ Failed to read {filename}: {str(e)}")
                continue

    if total_rows == 0:
        print("  ✗ No data written to CSV.")
        return None

    size_mb = os.path.getsize(COMBINED_CSV) / 1024 / 1024
    print(f"\n  ✅ Combined CSV ready: {total_rows:,} total rows, {size_mb:.1f} MB")

    return COMBINED_CSV

# =============================================================================
# GOOGLE DRIVE AUTHENTICATION (OAuth Refresh Token)
# =============================================================================

def authenticate_drive():
    """Authenticate using OAuth Refresh Token (Personal Account)"""
    try:
        print(f"  🔍 Checking env vars:")
        print(f"     GOOGLE_REFRESH_TOKEN: {'✅ Set' if os.environ.get('GOOGLE_REFRESH_TOKEN') else '❌ Missing'}")
        print(f"     GOOGLE_CLIENT_ID: {'✅ Set' if os.environ.get('GOOGLE_CLIENT_ID') else '❌ Missing'}")
        print(f"     GOOGLE_CLIENT_SECRET: {'✅ Set' if os.environ.get('GOOGLE_CLIENT_SECRET') else '❌ Missing'}")

        refresh_token = os.environ.get('GOOGLE_REFRESH_TOKEN')
        client_id = os.environ.get('GOOGLE_CLIENT_ID')
        client_secret = os.environ.get('GOOGLE_CLIENT_SECRET')

        if not refresh_token:
            print("  ⚠️ No GOOGLE_REFRESH_TOKEN env var - skipping upload")
            return None
        if not client_id:
            print("  ⚠️ No GOOGLE_CLIENT_ID env var - skipping upload")
            return None
        if not client_secret:
            print("  ⚠️ No GOOGLE_CLIENT_SECRET env var - skipping upload")
            return None

        creds = Credentials(
            None,
            refresh_token=refresh_token,
            token_uri="https://oauth2.googleapis.com/token",
            client_id=client_id,
            client_secret=client_secret,
            scopes=['https://www.googleapis.com/auth/drive']
        )

        service = build('drive', 'v3', credentials=creds)
        print("  ✅ Google Drive authenticated via OAuth")
        return service

    except Exception as e:
        print(f"  ⚠️ Drive auth failed: {str(e)}")
        return None

# =============================================================================
# UPLOAD TO GOOGLE DRIVE
# =============================================================================

def upload_to_drive(service, drive_folder_id, newly_downloaded):
    """Upload new xlsx files + refreshed combined CSV to Google Drive"""
    if not service:
        print("  ⚠️ No Drive service - skipping upload")
        return 0

    uploaded = 0

    # Only upload xlsx files if new ones were downloaded this run
    if newly_downloaded > 0:
        for filename in os.listdir(DOWNLOAD_DIR):
            if not filename.endswith('.xlsx'):
                continue
            try:
                filepath = os.path.join(DOWNLOAD_DIR, filename)
                file_metadata = {'name': filename, 'parents': [drive_folder_id]}
                media = MediaFileUpload(
                    filepath,
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
                service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id',
                    supportsAllDrives=True
                ).execute()
                print(f"  ✓ Uploaded: {filename}")
                uploaded += 1
            except Exception as e:
                print(f"  ✗ Upload failed {filename}: {str(e)}")

    # Always replace the combined CSV on Drive with the latest version
    if os.path.exists(COMBINED_CSV):
        try:
            csv_name = "DGI_COMBINED.csv"

            existing = service.files().list(
                q=f"'{drive_folder_id}' in parents and name='{csv_name}'",
                fields="files(id)",
                supportsAllDrives=True
            ).execute().get('files', [])

            for f in existing:
                service.files().delete(fileId=f['id'], supportsAllDrives=True).execute()
                print(f"  🗑 Removed old {csv_name} from Drive")

            file_metadata = {'name': csv_name, 'parents': [drive_folder_id]}
            media = MediaFileUpload(COMBINED_CSV, mimetype='text/csv')
            service.files().create(
                body=file_metadata,
                media_body=media,
                fields='id',
                supportsAllDrives=True
            ).execute()
            print(f"  ✓ Uploaded: {csv_name}")
            uploaded += 1

        except Exception as e:
            print(f"  ✗ Upload failed for combined CSV: {str(e)}")

    return uploaded

# =============================================================================
# CLEANUP OLD FILES (Via Drive API)
# =============================================================================

def cleanup_old_files(service, drive_folder_id, cutoff_date):
    """Delete Excel files older than 5 years from Google Drive via API"""
    if not service:
        print("  ⚠️ No Drive service - skipping cleanup")
        return 0, 0

    deleted = 0
    kept = 0

    try:
        results = service.files().list(
            q=f"'{drive_folder_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'",
            fields="files(id, name)",
            supportsAllDrives=True
        ).execute()

        for file in results.get('files', []):
            filename = file['name']
            file_id = file['id']
            parsed = parse_filename_to_date(filename)
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
                kept += 1

    except Exception as e:
        print(f"  ⚠️ Cleanup error: {str(e)}")

    return kept, deleted

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    print("=" * 70)
    print("DGI Cameroon - GitHub Actions Automation (OAuth)")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

    DRIVE_FOLDER_ID = os.environ.get('DRIVE_FOLDER_ID', '')

    cutoff_date = datetime.now() - timedelta(days=YEARS_TO_KEEP*365)
    print(f"📅 Data window: {cutoff_date.strftime('%Y-%m')} → {datetime.now().strftime('%Y-%m')}")

    months_to_process = get_month_list()
    print(f"📊 Will process {len(months_to_process)} months")

    # Step 1: Download files in parallel
    downloaded, skipped, failed = download_all_parallel(months_to_process)

    # Step 2: Combine into CSV (skips rebuild if nothing new)
    csv_path = combine_to_csv(newly_downloaded=downloaded)

    # Step 3: Authenticate Drive
    print("\n🔐 Authenticating Google Drive...")
    drive_service = authenticate_drive()

    # Step 4: Upload + Cleanup
    if drive_service and DRIVE_FOLDER_ID:
        print("\n📤 Uploading to Google Drive...")
        uploaded = upload_to_drive(drive_service, DRIVE_FOLDER_ID, newly_downloaded=downloaded)
        print(f"   Uploaded: {uploaded} files")

        print("\n🧹 Cleaning up files older than 5 years...")
        kept, deleted = cleanup_old_files(drive_service, DRIVE_FOLDER_ID, cutoff_date)
        print(f"   Kept: {kept}, Deleted: {deleted}")
    else:
        print("\n⚠️ Skipping Drive operations (no credentials or folder ID)")

    # Final summary
    print("\n" + "=" * 70)
    print("✅ EXECUTION COMPLETE")
    print(f"   Downloaded: {downloaded} new files")
    print(f"   Skipped:    {skipped} (already existed)")
    print(f"   Failed:     {failed} (not found or error)")
    if csv_path:
        size_mb = os.path.getsize(csv_path) / 1024 / 1024
        print(f"   Combined CSV: {size_mb:.1f} MB → {csv_path}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

if __name__ == "__main__":
    main()
