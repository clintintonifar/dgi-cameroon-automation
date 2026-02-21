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
DOWNLOAD_BATCH_SIZE = 10
MAX_RETRIES = 5
RETRY_DELAY = 5

FRENCH_MONTHS = {
    1: 'JANVIER', 2: 'FEVRIER', 3: 'MARS', 4: 'AVRIL',
    5: 'MAI', 6: 'JUIN', 7: 'JUILLET', 8: 'AOUT',
    9: 'SEPTEMBRE', 10: 'OCTOBRE', 11: 'NOVEMBRE', 12: 'DECEMBRE'
}

# The canonical set of columns we keep (intersection of old+new, relevant ones)
# Old format had extra cols: VENTE_BOISSON, BOITE_POSTALE, TELEPHONE, FORME_JURIDIQUE,
#   REGION_ADMINISTRATIVE, DEPARTEMENT, VILLE, COMMUNE, QUARTIER, LIEUX_DIT, ETATNIU, EXERCICE, MOIS
# New format (2024+) has fewer cols.
# We keep only the columns present in BOTH formats, dropping legacy-only ones.
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

def is_file_within_window(filename, cutoff_date):
    """Check if file date is within 5-year window"""
    parsed = parse_filename_to_date(filename)
    if parsed is None:
        return True
    year, month = parsed
    file_date = datetime(year, month, 1)
    return file_date >= cutoff_date

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
    """Download single month's file with retry logic"""
    month_name = FRENCH_MONTHS[month]
    url = BASE_URL.format(month_name, year)
    filename = f"FICHIER_{month_name}_{year}.xlsx"
    filepath = os.path.join(DOWNLOAD_DIR, filename)

    if os.path.exists(filepath):
        print(f"  ✓ Skip: {filename} (already exists)")
        return True

    for attempt in range(MAX_RETRIES):
        try:
            headers = {
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 Chrome/120.0.0.0 Safari/537.36",
                "Accept": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet, application/octet-stream, */*",
                "Accept-Language": "en-US,en;q=0.9",
                "Connection": "keep-alive",
                "Cache-Control": "no-cache"
            }

            response = requests.get(url, headers=headers, timeout=60)

            if response.status_code == 200:
                with open(filepath, 'wb') as f:
                    f.write(response.content)
                print(f"  ✓ Downloaded: {filename} ({len(response.content)/1024/1024:.1f} MB)")
                return True
            elif response.status_code == 404:
                if attempt < MAX_RETRIES - 1:
                    wait_time = RETRY_DELAY + random.uniform(1, 3)
                    print(f"  ⚠ 404 (Attempt {attempt+1}/{MAX_RETRIES}), retrying in {wait_time:.1f}s...")
                    time.sleep(wait_time)
                else:
                    print(f"  ⚠ Not found after {MAX_RETRIES} attempts: {filename}")
                    return False
            else:
                print(f"  ✗ Failed: {filename} (HTTP {response.status_code})")
                return None
        except Exception as e:
            if attempt < MAX_RETRIES - 1:
                wait_time = RETRY_DELAY + random.uniform(1, 3)
                print(f"  ⚠ Error (Attempt {attempt+1}/{MAX_RETRIES}), retrying...")
                time.sleep(wait_time)
            else:
                print(f"  ✗ Error after {MAX_RETRIES} attempts: {filename} - {str(e)}")
                return None
    return False

# =============================================================================
# COMBINE ALL EXCEL FILES INTO ONE CSV
# =============================================================================

def normalize_columns(df):
    """
    Normalize column names (strip whitespace, uppercase) and keep only
    the canonical columns that exist in both old and new file formats.
    Drops legacy-only columns silently.
    """
    # Normalize column names
    df.columns = [str(c).strip().upper() for c in df.columns]

    # Some old files use 'N°' or 'N' as the row index column — drop it
    for drop_col in ['N°', 'N', 'Nº']:
        if drop_col in df.columns:
            df = df.drop(columns=[drop_col])

    # Keep only canonical columns that are actually present in this file
    cols_present = [c for c in CANONICAL_COLUMNS if c in df.columns]
    df = df[cols_present]

    # Add any missing canonical columns as NaN so all frames align
    for col in CANONICAL_COLUMNS:
        if col not in df.columns:
            df[col] = None

    # Reorder to canonical order
    df = df[CANONICAL_COLUMNS]

    return df


def combine_to_csv():
    """
    Read all downloaded .xlsx files, normalize columns to the modern schema,
    inject YEAR and MONTH columns, and write one combined CSV.
    Returns the path to the CSV or None on failure.
    """
    print("\n📊 Combining Excel files into single CSV...")

    all_frames = []
    xlsx_files = sorted([
        f for f in os.listdir(DOWNLOAD_DIR)
        if f.endswith('.xlsx') and f.startswith('FICHIER_')
    ])

    if not xlsx_files:
        print("  ⚠️ No Excel files found to combine.")
        return None

    for filename in xlsx_files:
        parsed = parse_filename_to_date(filename)
        if parsed is None:
            print(f"  ⚠ Skipping (unparseable name): {filename}")
            continue

        year, month = parsed
        filepath = os.path.join(DOWNLOAD_DIR, filename)

        try:
            # Read first sheet; header is on row 0
            df = pd.read_excel(filepath, sheet_name=0, dtype=str, engine='openpyxl')

            # Drop completely empty rows
            df = df.dropna(how='all')

            # Normalize columns & align to canonical schema
            df = normalize_columns(df)

            # Inject source year/month
            df['YEAR'] = year
            df['MONTH'] = month

            all_frames.append(df)
            print(f"  ✓ Read {len(df):,} rows from {filename}")

        except Exception as e:
            print(f"  ✗ Failed to read {filename}: {str(e)}")
            continue

    if not all_frames:
        print("  ✗ No data frames to combine.")
        return None

    combined = pd.concat(all_frames, ignore_index=True)

    # Clean up: strip whitespace from string columns
    for col in CANONICAL_COLUMNS:
        combined[col] = combined[col].astype(str).str.strip().replace('nan', '')

    # Final column order: YEAR, MONTH first for easy filtering
    final_cols = ['YEAR', 'MONTH'] + CANONICAL_COLUMNS
    combined = combined[final_cols]

    combined.to_csv(COMBINED_CSV, index=False, encoding='utf-8-sig')

    total_rows = len(combined)
    size_mb = os.path.getsize(COMBINED_CSV) / 1024 / 1024
    print(f"\n  ✅ Combined CSV ready: {total_rows:,} total rows, {size_mb:.1f} MB")
    print(f"     Path: {COMBINED_CSV}")

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

def upload_to_drive(service, drive_folder_id):
    """Upload downloaded xlsx files + combined CSV to Google Drive folder"""
    if not service:
        print("  ⚠️ No Drive service - skipping upload")
        return 0

    uploaded = 0

    # Upload all .xlsx files
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

    # Upload combined CSV (overwrite existing one by deleting first)
    if os.path.exists(COMBINED_CSV):
        try:
            csv_name = "DGI_COMBINED.csv"

            # Delete existing combined CSV in the folder if present
            existing = service.files().list(
                q=f"'{drive_folder_id}' in parents and name='{csv_name}'",
                fields="files(id)",
                supportsAllDrives=True
            ).execute().get('files', [])

            for f in existing:
                service.files().delete(fileId=f['id'], supportsAllDrives=True).execute()
                print(f"  🗑 Removed old {csv_name} from Drive")

            # Upload fresh combined CSV
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

        files = results.get('files', [])

        for file in files:
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
                kept += 1  # Keep unparseable files

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

    downloaded = 0
    skipped = 0
    failed = 0

    # Step 1: Download files
    print("\n📥 Downloading files...")
    for i, (year, month) in enumerate(months_to_process, 1):
        print(f"[{i}/{len(months_to_process)}] ", end="")
        result = download_file(year, month)

        if result is True:
            downloaded += 1
        elif result is False:
            skipped += 1
        else:
            failed += 1

        if i % DOWNLOAD_BATCH_SIZE == 0:
            print(f"  → Progress: {i}/{len(months_to_process)} months")
            time.sleep(1)

    # Step 2: Combine all Excel files into one CSV
    csv_path = combine_to_csv()

    # Step 3: Authenticate Drive
    print("\n🔐 Authenticating Google Drive...")
    drive_service = authenticate_drive()

    # Step 4: Upload + Cleanup
    if drive_service and DRIVE_FOLDER_ID:
        print("\n📤 Uploading to Google Drive...")
        uploaded = upload_to_drive(drive_service, DRIVE_FOLDER_ID)
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
    print(f"   Skipped:    {skipped} (not found)")
    print(f"   Failed:     {failed} (errors)")
    if csv_path:
        print(f"   Combined CSV: {csv_path}")
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

if __name__ == "__main__":
    main()