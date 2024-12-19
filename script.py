import json
import boto3
import os
import zipfile
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
import pymongo  # Import pymongo to work with MongoDB
from bson import ObjectId

# Configuration
BUCKET_NAME = 'assess-platform-prod-user-data'
SOURCE_PREFIX = 'proctoring/images/camera/u/'
DESTINATION_PREFIX = 'proctoring/images/camera/u-glacier/'
LOCAL_TEMP_DIR = 'temp/'
SEGMENT_SIZE = 10 * 1024 * 1024 * 1024  # 1GB in bytes

# Initialize S3 client pass region and key and secret as well
s3 = boto3.client('s3')

def update_history_file(first_id, last_id, total):
    """Update or create history.json file with new IDs."""
    history_path = 'history.json'
    try:
        # Try to open the history file if it exists
        with open(history_path, 'r') as file:
            history_data = json.load(file)
    except FileNotFoundError:
        # If the file does not exist, start a new list
        history_data = []

    # Convert ObjectId to string and append new object with first_id and last_id
    history_data.append({'first_id': str(first_id), 'last_id': str(last_id), 'total':total})

    # Write the updated history back to the file
    with open(history_path, 'w') as file:
        json.dump(history_data, file, indent=4)

    print(f"Updated history file with first_id: {first_id} and last_id: {last_id}")



def get_files_to_download(bucket_name, prefixes):
    """List files under the given S3 prefixes."""
    files = []
    paginator = s3.get_paginator('list_objects_v2')
    
    for prefix in prefixes:  # Iterate over each prefix in the array
        for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
            for item in page.get('Contents', []):
                files.append(item)

    return files




def download_segment(files, local_dir):
    """Download files until the target size is reached, preserving directory structure, using concurrent downloads."""
    total_size = 0
    downloaded_files = []
    print(f"Downloading segment...")

    # Function to download a single file
    def download_file(file):
        key = file['Key']
        size = file['Size']
        local_path = os.path.join(local_dir, key[len(SOURCE_PREFIX):])
        os.makedirs(os.path.dirname(local_path), exist_ok=True)
        s3.download_file(BUCKET_NAME, key, local_path)
        return local_path, size

    # Using ThreadPoolExecutor to download files concurrently
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_file = {executor.submit(download_file, file): file for file in files}
        for future in as_completed(future_to_file):
            local_path, size = future.result()
            total_size += size
            downloaded_files.append(local_path)

    print(f"Segment size: {total_size / 1e9:.2f} GB")
    return downloaded_files, total_size

def compress_files(files, output_zip):
    """Compress a list of files into a ZIP archive, preserving directory structure."""
    with zipfile.ZipFile(output_zip, 'w', zipfile.ZIP_DEFLATED) as zipf:
        print(f"Compressing files into {output_zip}...")
        for file in files:
            arcname = file[len(LOCAL_TEMP_DIR):]  # Relative path within the ZIP
            print(f"Adding {file} to ZIP archive as {arcname}...")
            zipf.write(file, arcname)

def upload_to_s3(zip_path, bucket_name, destination_prefix, files):
    """Upload a ZIP file to S3 with Glacier Deep Archive storage class."""
    print(f"Uploading {zip_path} to S3...")
    file_name = os.path.basename(zip_path)
    destination_key = f"{destination_prefix}{file_name}"
    try:
        s3.upload_file(zip_path, bucket_name, destination_key, ExtraArgs={'StorageClass': 'DEEP_ARCHIVE'})
        print(f"Upload complete: s3://{bucket_name}/{destination_key}")
        delete_files_from_s3(bucket_name, files)
    except Exception as e:
        print(f"Upload failed: {e}")

def delete_files_from_s3(bucket_name, files):
    """Delete files in batch from S3."""
    # Do it in batches of 1000 files to avoid exceeding the limit
    batch_size = 1000
    for i in range(0, len(files), batch_size):
        batch_files = files[i:i+batch_size]
        objects_to_delete = [{'Key': SOURCE_PREFIX + file[len(LOCAL_TEMP_DIR):]} for file in batch_files]
        response = s3.delete_objects(Bucket=bucket_name, Delete={'Objects': objects_to_delete})
        deleted = response.get('Deleted', [])
        print(f"Deleted {len(deleted)} files from S3.")

def cleanup_local_files(files, zip_path):
    for file in files:
        if os.path.exists(file):
            os.remove(file)
            print(f"Deleted file: {file}")
        else:
            print(f"File not found, skipping deletion: {file}")
    # Optionally remove the zip file itself after uploading
    if os.path.exists(zip_path):
        os.remove(zip_path)
        print(f"Deleted zip file: {zip_path}")

def main():
    os.makedirs(LOCAL_TEMP_DIR, exist_ok=True)
    total_downloaded = 0


    client = pymongo.MongoClient("mongodb+srv://Amanullah:7q8cxiYuxV7ppAoA@assess-prod-dedicated.em5zz.mongodb.net/assess-production?readPreference=secondary")
    db = client.get_database("assess-production")
    
    proctoringmedias = db.get_collection("proctoringmedias")
    # fetch history.json file if exists pick last object and get last_id
    try:
        with open('history.json', 'r') as file:
            history_data = json.load(file)
            first_id = history_data[-1]['last_id']
            print(f"Retrieved last_id from history file: {first_id}")
    except FileNotFoundError:
        print("No history file found, starting from the beginning.")
        first_id = None
    
    print(f"{first_id} is the first_id")
    findQuery = {"_id": {"$gt": ObjectId(first_id)}} if first_id else {}  # If first_id is None
    print(f"Query: {findQuery}")
    proctoring_medias = list(proctoringmedias.find(findQuery, {'singleAssessmentUser': 1}).sort({"_id":1}).limit(1000))
    
    print(f"Retrieved {len(proctoring_medias)} user documents")
    if(len(proctoring_medias) == 0):
        print("No new documents found, exiting.")
        return
    update_history_file(proctoring_medias[0]['_id'], proctoring_medias[-1]['_id'], total=len(proctoring_medias))    
    
    source_prefixes = [f"proctoring/images/camera/u/{proctoring_media['singleAssessmentUser']}/" for proctoring_media in proctoring_medias]
    print(f"Source prefixes: {source_prefixes}")
    files = get_files_to_download(BUCKET_NAME, source_prefixes)
    print(f"Total files to download: {len(files)}")
    
    zip_file_name = f"{proctoring_medias[0]['_id']}-{proctoring_medias[-1]['_id']}.zip" # Use the first and last document IDs as the ZIP file name
    print(f"Starting download and upload process...")
    
    segment_files, segment_size = download_segment(files, LOCAL_TEMP_DIR)
    if not segment_files:
        print("No files to download, exiting.")
        return
    
    # Determine ZIP file name
    # zip_name = f"{"some"}_to_{"some"}.zip"
    
    zip_path = os.path.join(LOCAL_TEMP_DIR, zip_file_name)

    # Compress and upload
    compress_files(segment_files, zip_path)
    upload_to_s3(zip_path, BUCKET_NAME, DESTINATION_PREFIX, segment_files)

    # Cleanup
    cleanup_local_files(segment_files, zip_path)

    # Update progress and remaining files
    total_downloaded += segment_size
    files = files[len(segment_files):]
    print(f"Segment completed: {zip_file_name} ({segment_size / 1e9:.2f} GB)")
    # uncomment to process only one segment
    # break

    print(f"All files processed. Total downloaded: {total_downloaded / 1e9:.2f} GB")

if __name__ == "__main__":
    main()