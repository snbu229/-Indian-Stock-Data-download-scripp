import boto3
import os
import pandas as pd

# S3 bucket and folder details
bucket_name = "desiquant"n 
source_prefix = "data/"  # Root folder prefix from which to fetch files
destination_folder = "./data/"  # Local folder to save the files
tracking_file = "downloaded_files.txt"  # File to track downloaded files

# S3 credentials
s3 = boto3.client(
    "s3",
    aws_access_key_id="5c8ea9c516abfc78987bc98c70d2868a",
    aws_secret_access_key="0cf64f9f0b64f6008cf5efe1529c6772daa7d7d0822f5db42a7c6a1e41b3cadf",
    endpoint_url="https://cbabd13f6c54798a9ec05df5b8070a6e.r2.cloudflarestorage.com",
)

# Ensure destination folder exists
os.makedirs(destination_folder, exist_ok=True)

# Load or initialize the tracking file
if os.path.exists(tracking_file):
    with open(tracking_file, "r") as f:
        downloaded_files = set(f.read().splitlines())
else:
    downloaded_files = set()

print(f"Loaded {len(downloaded_files)} already downloaded files from tracking file.")

def is_empty_file(file_path):
    """Check if a file is empty."""
    return os.path.exists(file_path) and os.path.getsize(file_path) == 0

def download_files_from_s3(bucket_name, prefix, destination_folder):
    continuation_token = None
    while True:
        list_objects_params = {'Bucket': bucket_name, 'Prefix': prefix}
        if continuation_token:
            list_objects_params['ContinuationToken'] = continuation_token
        
        # List objects with pagination
        response = s3.list_objects_v2(**list_objects_params)
        
        if "Contents" in response:
            for obj in response["Contents"]:
                key = obj["Key"]
                size = obj["Size"]

                # Skip files already downloaded or empty files
                if key in downloaded_files or size == 0:
                    print(f"Skipping file: {key} (Already downloaded or empty)")
                    continue
                
                if key.endswith(".parquet.gz"):
                    file_name = os.path.join(destination_folder, os.path.relpath(key, source_prefix))
                    os.makedirs(os.path.dirname(file_name), exist_ok=True)

                    # Download the file
                    print(f"Downloading {key} to {file_name}...")
                    try:
                        s3.download_file(bucket_name, key, file_name)
                    except Exception as e:
                        print(f"Error downloading {key}: {e}")
                        continue

                    # Check if the downloaded file is empty
                    if is_empty_file(file_name):
                        print(f"Downloaded file is empty: {file_name}. Skipping conversion.")
                        os.remove(file_name)
                        continue

                    # Convert Parquet to CSV
                    csv_file = file_name.replace(".parquet.gz", ".csv")
                    print(f"Converting {file_name} to {csv_file}...")
                    try:
                        df = pd.read_parquet(file_name)
                        df.to_csv(csv_file, index=False)
                        print(f"Converted {file_name} to {csv_file}")
                        os.remove(file_name)  # Delete Parquet file after conversion
                        print(f"Deleted {file_name} after conversion.")
                    except Exception as e:
                        print(f"Error processing {file_name}: {e}")
                        continue

                    # Mark the file as downloaded
                    downloaded_files.add(key)
                    with open(tracking_file, "a") as f:
                        f.write(f"{key}\n")
        
        # Check if the listing was truncated (more files available)
        if response.get("IsTruncated"):
            continuation_token = response.get("NextContinuationToken")
        else:
            break

# Download all files from S3, including from multiple folders
download_files_from_s3(bucket_name, source_prefix, destination_folder)

print("All files have been downloaded and converted.")
