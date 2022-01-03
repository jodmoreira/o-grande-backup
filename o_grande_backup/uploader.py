import aws_tools.s3_tools as s3_tools
import os
from datetime import datetime
import pytz
import telegram_tools.telegram_tools as telegram_tools

## List all files in the twitter directory
def twitter_files():
    now = datetime.now()
    day = now.day
    if len(str(day)) == 1:
        day = f"0{now.day}"
    month = now.month
    if len(str(month)) == 1:
        month = f"0{now.month}"
    year = now.year
    bsb_tz = pytz.timezone("America/Sao_Paulo")
    ingestion_datetime = bsb_tz.localize(now).strftime("%Y-%m-%d %H:%M:%S%z")
    script_path = os.path.dirname(os.path.realpath(__file__))
    twitter_files_path = f"{script_path}/twitter_tools/temp_storage/compressed_files/"
    files = os.listdir(twitter_files_path)

    for file in files:
        post_lake_dir = f"social_media/twitter/landing_zone/year={year}/month={month}/day={day}/{file}"
        content = f"{twitter_files_path}/{file}"
        request = s3_tools.upload_compressed_file_from_local_directory(
            "ogb-lake", content, post_lake_dir
        )
        if request == 200:
            print(f"{file} uploaded successfully")
            print(f"removing {file} from local directory")
            telegram_tools.send_message(f"{file} uploaded successfully")
            os.remove(f"{twitter_files_path}/{file}")


if __name__ == "__main__":
    twitter_files()
