"""
Lambda function that combines Part 1 (BLS file sync) and Part 2 (API data fetch).
This function runs daily via EventBridge schedule.
"""
import boto3
import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import time
import hashlib
import json
from datetime import datetime
import os

# AWS Setup
s3 = boto3.client('s3')
bucket_name = os.environ.get('BUCKET_NAME', 'bls-dataset-sync2')

# Remote source URL for BLS data
data_url = 'https://download.bls.gov/pub/time.series/pr/'

# API URL for population data
api_url = 'https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population'

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


# ========== PART 1: BLS File Sync Functions ==========

def get_remote_files(max_retries=3, retry_delay=5):
    """Fetch file list from the remote directory with retry logic for 403 errors."""
    for attempt in range(max_retries):
        try:
            response = session.get(data_url, timeout=30)
            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')
                files = []
                for link in soup.find_all('a'):
                    file_name = link.get('href')
                    if file_name and file_name != '../' and not file_name.endswith('/'):
                        files.append(file_name)
                return files
            elif response.status_code == 403:
                print(f"403 Forbidden error (attempt {attempt + 1}/{max_retries})")
                if attempt < max_retries - 1:
                    print(f"Waiting {retry_delay} seconds before retry...")
                    time.sleep(retry_delay)
                    session.get('https://www.bls.gov/', timeout=30)
                    retry_delay *= 2
                else:
                    print("Max retries reached. Check BLS data access policies.")
                    return []
            else:
                print(f"Failed to fetch remote files. Status code: {response.status_code}")
                return []
        except requests.exceptions.RequestException as e:
            print(f"Error fetching remote files (attempt {attempt + 1}/{max_retries}): {e}")
            if attempt < max_retries - 1:
                time.sleep(retry_delay)
            else:
                return []
    return []


def get_s3_files():
    """Get list of all files currently in S3 bucket."""
    try:
        response = s3.list_objects_v2(Bucket=bucket_name)
        if 'Contents' in response:
            return {obj['Key']: obj['ETag'].strip('"') for obj in response['Contents']}
        return {}
    except Exception as e:
        print(f"Error listing S3 files: {e}")
        return {}


def file_needs_update(file_name, file_url, s3_etag):
    """Check if file needs updating by comparing remote MD5 with S3 ETag."""
    try:
        response = session.get(file_url, timeout=60, stream=True)
        if response.status_code == 200:
            md5_hash = hashlib.md5()
            file_content = b''
            for chunk in response.iter_content(chunk_size=8192):
                if chunk:
                    md5_hash.update(chunk)
                    file_content += chunk
            remote_md5 = md5_hash.hexdigest()
            s3_md5 = s3_etag.strip('"')
            needs_update = remote_md5 != s3_md5
            return needs_update, file_content if needs_update else None
        else:
            return True, None
    except Exception as e:
        print(f"Error checking file {file_name}: {e}")
        return True, None


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


def sync_files():
    """Sync files between remote source and S3."""
    print("Starting BLS file sync process...")
    
    remote_files = get_remote_files()
    if not remote_files:
        print("No remote files found or failed to fetch file list.")
        return
    
    print(f"Found {len(remote_files)} files on remote source.")
    
    s3_files = get_s3_files()
    print(f"Found {len(s3_files)} files in S3 bucket.")
    
    remote_file_set = set(remote_files)
    s3_file_set = set(s3_files.keys())
    
    # Handle new and updated files
    for file_name in remote_files:
        file_url = urljoin(data_url, file_name)
        if file_name not in s3_files:
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
            needs_update, file_content = file_needs_update(file_name, file_url, s3_files[file_name])
            if needs_update:
                if file_content:
                    print(f"File changed detected: {file_name}")
                    upload_to_s3(file_name, file_content)
                else:
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
    
    # Handle deleted files
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
    
    print("BLS file sync process completed!")


# ========== PART 2: API Data Fetch Functions ==========

def fetch_data_from_api(url):
    """Fetch data from the API."""
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()
        else:
            print(f"Failed to fetch data: HTTP {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None


def upload_json_to_s3(data, bucket, file_name):
    """Upload JSON data to S3."""
    try:
        json_data = json.dumps(data)
        s3.put_object(Bucket=bucket, Key=file_name, Body=json_data)
        print(f"Data successfully uploaded to {bucket}/{file_name}")
        return True
    except Exception as e:
        print(f"Error uploading to S3: {e}")
        return False


def fetch_api_data():
    """Fetch population data from API and upload to S3."""
    print("Starting API data fetch process...")
    
    data = fetch_data_from_api(api_url)
    
    if data:
        file_name = f"population_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        success = upload_json_to_s3(data, bucket_name, file_name)
        if success:
            print("API data fetch process completed!")
            return file_name
        else:
            print("API data fetch process failed during upload.")
            return None
    else:
        print("API data fetch process failed - no data received.")
        return None


# ========== Lambda Handler ==========

def handler(event, context):
    """
    Main Lambda handler that executes Part 1 (BLS sync) and Part 2 (API fetch).
    """
    print("=" * 60)
    print("Starting combined sync and fetch process")
    print("=" * 60)
    
    try:
        # Execute Part 1: Sync BLS files
        sync_files()
        
        # Execute Part 2: Fetch API data
        json_file = fetch_api_data()
        
        print("=" * 60)
        print("Combined sync and fetch process completed successfully")
        print("=" * 60)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Sync and fetch completed successfully',
                'json_file_created': json_file
            })
        }
    except Exception as e:
        print(f"Error in handler: {e}")
        import traceback
        traceback.print_exc()
        return {
            'statusCode': 500,
            'body': json.dumps({
                'message': f'Error: {str(e)}'
            })
        }
