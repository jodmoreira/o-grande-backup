import boto3
from datetime import datetime
import json
import botocore
import time

s3 = boto3.client("s3")


def list_files(bucket_name, prefix, max_keys):
    all_objects = s3.list_objects_v2(
        Bucket=bucket_name, Prefix=prefix, MaxKeys=max_keys
    )
    keys = [i.get("Key") for i in all_objects.get("Contents")]
    return keys


def upload_file(bucket_name, content, post_lake_dir):
    try:
        s3.put_object(
            Body=(bytes(json.dumps(content).encode("UTF-8"))),
            Bucket=bucket_name,
            Key=post_lake_dir,
        )
    except botocore.exceptions.NoCredentialsError as e:
        print(e)
        time.sleep(1)
        upload_file(bucket_name, content, post_lake_dir)


def download_file(bucket_name, key, file_path):
    s3.download_file(bucket_name, key, file_path)
