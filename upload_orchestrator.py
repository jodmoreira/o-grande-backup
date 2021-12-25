import aws_tools.s3_tools as s3_tools
import os
from datetime import datetime
import pytz

## List all files in the twitter directory
def twitter_files():
    now = datetime.now()
    bsb_tz = pytz.timezone("America/Sao_Paulo")
    ingestion_datetime = bsb_tz.localize(now).strftime("%Y-%m-%d %H:%M:%S%z")
    script_path = os.path.dirname(os.path.realpath(__file__))
    twitter_files_path = f"{script_path}/twitter_tools/temp_storage/compressed_files/"
    files = os.listdir(twitter_files_path)

    for file in files:
        post_lake_dir = f"social_media/twitter/landing_zone/year={now.year}/month={now.month}/day={now.day}/{file}"
        print(post_lake_dir)
        content = f"{twitter_files_path}/{file}"
        request = s3_tools.upload_compressed_file_from_local_directory(
            "ogb-lake", content, post_lake_dir
        )
        print(request)
        if request == 200:
            print(f"{file} uploaded successfully")
            print(f"removing {file} from local directory")
            os.remove(f"{twitter_files_path}/{file}")


if __name__ == "__main__":
    twitter_files()
