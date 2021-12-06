import boto3
from datetime import datetime

cli = boto3.client("s3")


def list_files(bucket_name, prefix, max_keys):
    response = cli.list_objects(Bucket=bucket_name, Prefix=prefix, MaxKeys=max_keys)
    all_files = [i for i in response["Contents"] if "Contents" in response]
    return all_files


def upload_file(bucket_name, file_path, key):
    cli.upload_file(file_path, bucket_name, key)


def download_file(bucket_name, key, file_path):
    cli.download_file(bucket_name, key, file_path)
