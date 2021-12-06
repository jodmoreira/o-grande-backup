import boto3
from datetime import datetime

s3 = boto3.client("s3")


def list_files(bucket_name, prefix, max_keys):
    all_objects = s3.list_objects_v2(
        Bucket=bucket_name, Prefix=prefix, MaxKeys=max_keys
    )
    keys = [i.get("Key") for i in all_objects.get("Contents")]
    return keys


def list_agents(social_media_name, bucket_name):
    all_agents = []
    prefix_social_media = f"social_media/{social_media_name}/"
    years = list_files(bucket_name, prefix_social_media, 1000)
    for year in years:
        prefix_year = f"{prefix_social_media}/{year}/"
        months = list_files(bucket_name, prefix_year, 1000)
        for month in months:
            prefix_month = f"{prefix_year}/{month}/"
            days = list_files(bucket_name, prefix_month, 1000)
            for day in days:
                prefix_day = f"{prefix_month}/{day}/"
                agents = list_files(bucket_name, prefix_day, 1000)
                all_agents.append(agents)
    return all_agents


def upload_file(bucket_name, file_path, key):
    s3.upload_file(file_path, bucket_name, key)


def download_file(bucket_name, key, file_path):
    s3.download_file(bucket_name, key, file_path)
