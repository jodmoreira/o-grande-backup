import aws_tools.s3_tools as s3_tools
import os
from datetime import datetime
import pytz
import telegram_tools.telegram_tools as telegram_tools
import json

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
        file = read_json_file(file)
        post_lake_dir = f"social_media/twitter/landing_zone/year={year}/month={month}/day={day}/{file}"
        content = f"{twitter_files_path}/{file}"
        file_size = os.path.getsize(content)
        request = s3_tools.upload_compressed_file_from_local_directory(
            "ogb-lake", content, post_lake_dir
        )
        if request == 200:
            print(f"{file} uploaded successfully")
            print(f"removing {file} from local directory")
            telegram_tools.send_message(
                f"The {file_size} bytes file uploaded successfully at {datetime.now()}"
            )
            os.remove(f"{twitter_files_path}/{file}")


def read_json_file(file):
    with open(file, "r") as f:
        rows = json.load(f)
        output = {}
        for row in rows:
            output["post_platform_id"] = row["post_platform_id"]
            output["post_date"] = row["post_date"]
            output["ingestion_datetime"] = row["ingestion_datetime"]
            output[
                "post_lake_dir"
            ] = (
                post_lake_dir
            ) = f"social_media/twitter/landing_zone/year={year}/month={month}/day={day}/{file}"
            output["twitter_profile_id"] = row["twitter_profile_id"]
            write_to_db(output)


def write_to_db(data):
    now = datetime.now()
    bsb_tz = pytz.timezone("America/Sao_Paulo")
    ingestion_datetime = bsb_tz.localize(now).strftime("%Y-%m-%d %H:%M:%S%z")
    day = now.day
    if len(str(day)) == 1:
        day = f"0{now.day}"
    month = now.month
    if len(str(month)) == 1:
        month = f"0{now.month}"
    year = now.year
    postgres_tools.add_new_twitter_post_non_agent(
        data["post_platform_id"],
        data["post_date"],
        data["ingestion_datetime"],
        data["post_lake_dir"],
        data["twitter_profile_id"],
    )


if __name__ == "__main__":
    twitter_files()
