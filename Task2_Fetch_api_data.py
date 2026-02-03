import boto3
import requests
import json
from datetime import datetime

s3 = boto3.client('s3')
bucket_name = 'bls-dataset-sync2'  

# API URL to fetch data
api_url = 'https://honolulu-api.datausa.io/tesseract/data.jsonrecords?cube=acs_yg_total_population_1&drilldowns=Year%2CNation&locale=en&measures=Population'

# Function to fetch data from the API
def fetch_data_from_api(url):
    try:
        response = requests.get(url)
        if response.status_code == 200:
            return response.json()  # Parse JSON response and return it
        else:
            print(f"Failed to fetch data: HTTP {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return None

# Function to upload data to S3
def upload_to_s3(data, bucket, file_name):
    try:
        json_data = json.dumps(data)
        
        s3.put_object(Bucket=bucket, Key=file_name, Body=json_data)
        print(f"Data successfully uploaded to {bucket}/{file_name}")
    except Exception as e:
        print(f"Error uploading to S3: {e}")

# Main function to execute the workflow
def main():
    data = fetch_data_from_api(api_url)
    
    if data:
        file_name = f"population_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        
        upload_to_s3(data, bucket_name, file_name)

if __name__ == "__main__":
    main()
