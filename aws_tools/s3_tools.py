import boto3
from datetime import datetime

s3 = boto3.client("s3")


def list_files(bucket_name, prefix, max_keys):
    all_objects = s3.list_objects_v2(
        Bucket=bucket_name, Prefix=prefix, MaxKeys=max_keys
    )
    keys = [i.get("Key") for i in all_objects.get("Contents")]
    return keys


# Need to work with a paginator
# def list_files(bucket_name):
#     all_agents = []
#     prefix_social_media = f"social_media/{social_media_name}/"
#     return all_agents


def upload_file(bucket_name, file_path, key):
    s3.upload_file(file_path, bucket_name, key)


def download_file(bucket_name, key, file_path):
    s3.download_file(bucket_name, key, file_path)
