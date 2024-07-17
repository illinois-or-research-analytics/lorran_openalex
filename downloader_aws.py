import boto3
import os
from tqdm import tqdm
from botocore import UNSIGNED
from botocore.client import Config

# Create an S3 client with unsigned requests
s3 = boto3.client('s3', config=Config(signature_version=UNSIGNED))

# List contents of the bucket
bucket = 'openalex'
prefix = 'data/works/'
response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

# Ensure the local 'AWS' directory exists
local_directory = 'AWS'
if not os.path.exists(local_directory):
    os.makedirs(local_directory)

# Download each file to the local 'data' directory with a progress bar
for obj in tqdm(response.get('Contents', []), desc="Downloading files"):
    key = obj['Key']
    local_file_path = os.path.join(local_directory, key[len(prefix):])
    local_file_dir = os.path.dirname(local_file_path)
    if not os.path.exists(local_file_dir):
        os.makedirs(local_file_dir)
    s3.download_file(bucket, key, local_file_path)
    print(f'Downloaded {key} to {local_file_path}')
