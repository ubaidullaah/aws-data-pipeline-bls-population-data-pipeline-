import boto3
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import hashlib

s3 = boto3.client('s3')
bucket_name = 'bls-dataset-sync2'  

# Remote source URL
data_url = 'https://download.bls.gov/pub/time.series/pr/'

# Create a session to manage cookies and headers for subsequent requests
session = requests.Session()

# Set headers with a User-Agent to mimic a browser
headers = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Language': 'en-US,en;q=0.9',
    'Accept-Encoding': 'gzip, deflate, br',
    'Connection': 'keep-alive',
    'Upgrade-Insecure-Requests': '1',
    'Referer': 'https://www.bls.gov/',
    'Sec-Fetch-Dest': 'document',
    'Sec-Fetch-Mode': 'navigate',
    'Sec-Fetch-Site': 'same-site',
    'Cache-Control': 'max-age=0',
}

# Set session headers to persist across requests
session.headers.update(headers)

# Fetch file list from the remote directory with retry logic for 403 errors
def get_remote_files(max_retries=3, retry_delay=5):
    # Use the session to send the request and maintain cookies or session data
    for attempt in range(max_retries):
        try:
            response = session.get(data_url, timeout=30)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                files = []
                for link in soup.find_all('a'):
                    file_name = link.get('href')
                    # Filter out directories (end with /) and parent directory links
                    if file_name and file_name != '../' and not file_name.endswith('/'):
                        files.append(file_name)
                return files
            elif response.status_code == 403:
                print(f"403 Forbidden error (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    print(f"Waiting {retry_delay} seconds before retry...")
                    time.sleep(retry_delay)
                    # Try visiting the main BLS page first to establish session
                    session.get('https://www.bls.gov/', timeout=30)
                    retry_delay *= 2  # Exponential backoff
                else:
                    print("Max retries reached. Check BLS data access policies.")
                    print(f"Response headers: {dict(response.headers)}")
                    print(f"Response text (first 500 chars): {response.text[:500]}")
                    return []
            else:
                print(f"Failed to fetch remote files. Status code: {response.status_code}")
                print(f"Response headers: {dict(response.headers)}")
                print(f"Response text (first 500 chars): {response.text[:500]}")
                return []
        except requests.exceptions.RequestException as e:
            print(f"Error fetching remote files (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                return []
    return []

# Get list of all files currently in S3 bucket
def get_s3_files():
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            return {obj['Key']: obj['ETag'].strip('"') for obj in response['Contents']}
        return {}
    except Exception as e:
        print(f"Error listing S3 files: {e}")
        return {}

# Check if file needs to be updated by comparing MD5 hashes
def file_needs_update(file_name, file_url, s3_etag):
    """
    Check if file needs updating by comparing remote MD5 with S3 ETag.
    S3 ETag for single-part uploads is the MD5 hash of the file content.
    Downloads file to compute MD5 for accurate comparison.
    Returns (needs_update: bool, file_content: bytes or None)
    """
    try:
        response = session.get(file_url, timeout=60, stream=True)
        if response.status_code == 200:
            # Download and compute MD5
            md5_hash = hashlib.md5()
            file_content = b''
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    md5_hash.update(chunk)
                    file_content += chunk
            remote_md5 = md5_hash.hexdigest()
            # S3 ETag is MD5 for single-part uploads 
            s3_md5 = s3_etag.strip('"')
            needs_update = remote_md5 != s3_md5
            return needs_update, file_content if needs_update else None
        else:
            return True, None
    except Exception as e:
        print(f"Error checking file {file_name}: {e}")
        # Error downloading, assume needs update
        return True, None

# Upload file content to S3
def upload_to_s3(file_name, file_content, max_retries=3):
    """Upload file content to S3."""
    for attempt in range(max_retries):
        try:
            print(f"Uploading {file_name} to S3...")
            s3.put_object(Bucket=bucket_name, Key=file_name, Body=file_content)
            print(f"{file_name} uploaded successfully.")
            return True
        except Exception as e:
            print(f"Error uploading {file_name} (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(5 * (attempt + 1))
            else:
                return False
    return False

# Sync files between remote source and S3
def sync_files():
    print("Starting sync process...")
    
    # Fetch the list of files from the remote source
    remote_files = get_remote_files()
    if not remote_files:
        print("No remote files found or failed to fetch file list.")
        return
    
    print(f"Found {len(remote_files)} files on remote source.")
    
    # Get list of files currently in S3 with their ETags
    s3_files = get_s3_files()
    print(f"Found {len(s3_files)} files in S3 bucket.")
    
    remote_file_set = set(remote_files)
    s3_file_set = set(s3_files.keys())
    
    # Handle new and updated files
    for file_name in remote_files:
        file_url = urljoin(data_url, file_name)
        if file_name not in s3_files:
            # New file - download and upload it
            print(f"New file detected: {file_name}")
            try:
                response = session.get(file_url, timeout=60)
                if response.status_code == 200:
                    upload_to_s3(file_name, response.content)
                else:
                    print(f"Failed to download {file_name}. Status code: {response.status_code}")
            except Exception as e:
                print(f"Error downloading {file_name}: {e}")
        else:
            # File exists - check if it needs updating
            needs_update, file_content = file_needs_update(file_name, file_url, s3_files[file_name])
            if needs_update:
                if file_content:
                    print(f"File changed detected: {file_name}")
                    upload_to_s3(file_name, file_content)
                else:
                    # File content not available, download again
                    print(f"File changed detected: {file_name} (downloading...)")
                    try:
                        response = session.get(file_url, timeout=60)
                        if response.status_code == 200:
                            upload_to_s3(file_name, response.content)
                        else:
                            print(f"Failed to download {file_name}. Status code: {response.status_code}")
                    except Exception as e:
                        print(f"Error downloading {file_name}: {e}")
            else:
                print(f"{file_name} is up to date, skipping.")
    
    # Handle deleted files - remove files from S3 that no longer exist on source
    files_to_delete = s3_file_set - remote_file_set
    if files_to_delete:
        print(f"\nFound {len(files_to_delete)} file(s) in S3 that no longer exist on source:")
        for file_name in files_to_delete:
            try:
                print(f"Deleting {file_name} from S3...")
                s3.delete_object(Bucket=bucket_name, Key=file_name)
                print(f"{file_name} deleted successfully.")
            except Exception as e:
                print(f"Error deleting {file_name}: {e}")
    else:
        print("\nNo files to delete - all S3 files exist on remote source.")
    
    print("\nSync process completed!")

# Run the sync function
if __name__ == "__main__":
    sync_files()
