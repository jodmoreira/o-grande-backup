import boto3
from datetime import datetime
import json
import botocore
import time
import base64
import hashlib

s3 = boto3.client("s3")


def list_files(bucket_name, prefix, max_keys=1000):
    """
    List files in a bucket.
    params:
    bucket_name'
    prefix
    """
    all_objects = s3.list_objects_v2(
        Bucket=bucket_name, Prefix=prefix, MaxKeys=max_keys
    )
    print(bucket_name, prefix, max_keys)
    keys = [i.get("Key") for i in all_objects.get("Contents")]
    return keys


def upload_compressed_file_from_local_directory(
    bucket_name, local_directory, post_lake_dir
):
    request = s3.upload_file(
        local_directory,
        bucket_name,
        post_lake_dir,
        ExtraArgs={"ContentType": "application/json"},
    )
    try:
        waiter = s3.get_waiter("object_exists")
        waiter.wait(
            Bucket=bucket_name,
            Key=post_lake_dir,
            WaiterConfig={"Delay": 2, "MaxAttempts": 5},
        )
        return 200
    except:
        time.sleep(1)
        upload_compressed_file_from_local_directory(
            bucket_name, local_directory, post_lake_dir
        )


def upload_file(bucket_name, content, post_lake_dir):
    try:
        request = s3.put_object(
            Body=(bytes(json.dumps(content).encode("UTF-8"))),
            Bucket=bucket_name,
            Key=post_lake_dir,
            ContentMD5=base64.b64encode(
                hashlib.md5(bytes(json.dumps(content).encode("UTF-8"))).digest()
            ).decode("UTF-8"),
        )
        return request["ResponseMetadata"]["HTTPStatusCode"]
    except botocore.exceptions.NoCredentialsError as e:
        print(e)
        time.sleep(1)
        upload_file(bucket_name, content, post_lake_dir)


def download_file(bucket_name, key, file_path):
    output = s3.download_file(bucket_name, key, file_path)
    return output
