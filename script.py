import boto3
import os
import zipfile
from datetime import datetime

# Configuration
BUCKET_NAME = 'assess-platform-prod-user-data'
SOURCE_PREFIX = 'proctoring/images/camera/u/'
DESTINATION_PREFIX = 'proctoring/images/camera/u-glacier/'
LOCAL_TEMP_DIR = 'temp/'
SEGMENT_SIZE = 10 * 1024 * 1024 * 1024  # 1GB in bytes

# Initialize S3 client pass region and key and secret as well
s3 = boto3.client('s3')



def get_files_to_download(bucket_name, prefix):
    """List all files under the given S3 prefix."""
    files = []
    paginator = s3.get_paginator('list_objects_v2') 
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        files.extend(page.get('Contents', []))
    return files


def download_segment(files, local_dir, target_size):
    """Download files until the target size is reached, preserving directory structure."""
    total_size = 0
    downloaded_files = []
    date_range = [None, None]
    print(f"Downloading segment...")
    for file in files:
        key = file['Key']
        size = file['Size']
        last_modified = file['LastModified']

        if total_size + size > target_size:
            break

        # Preserve directory structure
        local_path = os.path.join(local_dir, key[len(SOURCE_PREFIX):])  # Adjust path to include subdirectories
        os.makedirs(os.path.dirname(local_path), exist_ok=True)  # Create directories as needed

        # Download file
        s3.download_file(BUCKET_NAME, key, local_path)

        # Update size, range, and tracking
        total_size += size
        downloaded_files.append(local_path)
        if not date_range[0] or last_modified < date_range[0]:
            date_range[0] = last_modified
        if not date_range[1] or last_modified > date_range[1]:
            date_range[1] = last_modified

    print(f"Segment size: {total_size / 1e9:.2f} GB")
    return downloaded_files, total_size, date_range

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
        for file in files:
            original_key = SOURCE_PREFIX + file[len(LOCAL_TEMP_DIR):]
            s3.delete_object(Bucket=bucket_name, Key=original_key)
            print(f"Deleted original file from S3: {original_key}")
    except Exception as e:
        print(f"Upload failed: {e}")

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
    files = get_files_to_download(BUCKET_NAME, SOURCE_PREFIX)
    print(f"Total files to download: {len(files)}")
    total_downloaded = 0

    while files:
        # Download segment
        segment_files, segment_size, date_range = download_segment(files, LOCAL_TEMP_DIR, SEGMENT_SIZE)
        if not segment_files:
            break

        # Determine ZIP file name
        start_date = date_range[0].strftime('%Y-%m-%d')
        end_date = date_range[1].strftime('%Y-%m-%d')
        zip_name = f"{start_date}_to_{end_date}.zip"
        zip_path = os.path.join(LOCAL_TEMP_DIR, zip_name)

        # Compress and upload
        compress_files(segment_files, zip_path)
        upload_to_s3(zip_path, BUCKET_NAME, DESTINATION_PREFIX, segment_files)

        # Cleanup
        cleanup_local_files(segment_files, zip_path)

        # Update progress and remaining files
        total_downloaded += segment_size
        files = files[len(segment_files):]
        print(f"Segment completed: {zip_name} ({segment_size / 1e9:.2f} GB)")

    print(f"All files processed. Total downloaded: {total_downloaded / 1e9:.2f} GB")

if __name__ == "__main__":
    main()