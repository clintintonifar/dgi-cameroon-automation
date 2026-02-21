# =============================================================================
# DGI Cameroon - GitHub Actions Automation
# Rolling 5-Year Window: Always keep last 60 months of data
# Runs monthly via GitHub Actions, uploads to Google Drive via API
# =============================================================================

import os
import time
import requests
import shutil
from datetime import datetime, timedelta
from google.oauth2 import service_account
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

# 🔹 Fixed: Removed trailing spaces in URL
BASE_URL = "https://teledeclaration-dgi.cm/UploadedFiles/AttachedFiles/ArchiveListecontribuable/FICHIER%20{}%20{}.xlsx"

# 🔹 GitHub Actions uses /tmp for temp storage
DOWNLOAD_DIR = "/tmp/dgi_downloads"
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
        return True  # Keep unparseable files (safety)
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
# GOOGLE DRIVE AUTHENTICATION (API-Based)
# =============================================================================

def authenticate_drive():
    """Authenticate with Google Drive using service account from env var"""
    try:
        credentials_json = os.environ.get('GOOGLE_DRIVE_CREDENTIALS')
        if not credentials_json:
            print("⚠️ No GOOGLE_DRIVE_CREDENTIALS env var - skipping upload")
            return None
        
        # Save credentials to temp file
        cred_file = "/tmp/credentials.json"
        with open(cred_file, 'w') as f:
            f.write(credentials_json)
        
        # Authenticate with service account
        creds = service_account.Credentials.from_service_account_file(
            cred_file,
            scopes=['https://www.googleapis.com/auth/drive']
        )
        
        service = build('drive', 'v3', credentials=creds)
        print("✅ Google Drive authenticated via API")
        return service
        
    except Exception as e:
        print(f"⚠️ Drive auth failed: {str(e)}")
        return None

# =============================================================================
# UPLOAD TO GOOGLE DRIVE (API-Based)
# =============================================================================

def upload_to_drive(service, drive_folder_id):
    """Upload downloaded files to Google Drive folder via API"""
    if not service:
        print("⚠️ No Drive service - skipping upload")
        return 0
    
    uploaded = 0
    for filename in os.listdir(DOWNLOAD_DIR):
        if filename.endswith('.xlsx'):
            try:
                filepath = os.path.join(DOWNLOAD_DIR, filename)
                
                # Create file metadata
                file_metadata = {
                    'name': filename,
                    'parents': [drive_folder_id]
                }
                
                # Upload file
                media = MediaFileUpload(
                    filepath,
                    mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                )
                file = service.files().create(
                    body=file_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
                
                print(f"  ✓ Uploaded: {filename}")
                uploaded += 1
            except Exception as e:
                print(f"  ✗ Upload failed {filename}: {str(e)}")
    
    return uploaded

# =============================================================================
# CLEANUP OLD FILES (Via Drive API)
# =============================================================================

def cleanup_old_files(service, drive_folder_id, cutoff_date):
    """Delete files older than 5 years from Google Drive via API"""
    if not service:
        print("⚠️ No Drive service - skipping cleanup")
        return 0, 0
    
    deleted = 0
    kept = 0
    
    try:
        # List all Excel files in the target folder
        results = service.files().list(
            q=f"'{drive_folder_id}' in parents and mimeType='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'",
            fields="files(id, name)"
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
                    # Delete old file via API
                    service.files().delete(fileId=file_id).execute()
                    print(f"  🗑 Deleted old: {filename}")
                    deleted += 1
            else:
                kept += 1  # Keep unparseable files (safety)
                
    except Exception as e:
        print(f"⚠️ Cleanup error: {str(e)}")
    
    return kept, deleted

# =============================================================================
# MAIN EXECUTION
# =============================================================================

def main():
    print("=" * 70)
    print("DGI Cameroon - GitHub Actions Automation")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    # 🔹 Get Drive folder ID from environment variable (GitHub Secret)
    DRIVE_FOLDER_ID = os.environ.get('DRIVE_FOLDER_ID', '')
    
    cutoff_date = datetime.now() - timedelta(days=YEARS_TO_KEEP*365)
    print(f"📅 Data window: {cutoff_date.strftime('%Y-%m')} → {datetime.now().strftime('%Y-%m')}")
    
    months_to_process = get_month_list()
    print(f"📊 Will process {len(months_to_process)} months")
    
    downloaded = 0
    skipped = 0
    failed = 0
    
    # Download files
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
    
    # Authenticate Drive
    print("\n🔐 Authenticating Google Drive...")
    drive_service = authenticate_drive()
    
    # Upload + Cleanup (only if auth succeeded)
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
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)

if __name__ == "__main__":
    main()