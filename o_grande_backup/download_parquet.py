## This script download parquet files from the s3 bucket and stores it locally

import aws_tools.s3_tools as s3_tools
import os
from datetime import date


def download_twitter_today_files():
    """
    This function downloads the parquet files from the s3 bucket and stores it locally
    """
    year = date.today().year
    month = date.today().month
    if len(str(month)) == 1:
        month = "0" + str(month)
    day = date.today().day
    if len(str(day)) == 1:
        day = "0" + str(day)
    script_dir = os.path.dirname(os.path.realpath(__file__))
    list_s3_files = s3_tools.list_files(
        "ogb-lake",
        f"social_media/twitter/structured_zone/agent/year={year}/month={month}/day={day}/",
    )
    for file_name in list_s3_files:
        output_file = file_name.split("/")[-1]
        file_path = (
            f"{script_dir}/twitter_tools/temp_storage/parquet_files/{output_file}"
        )
        ## check if file already exists
        if not os.path.isfile(file_path):
            s3_tools.download_file(
                "ogb-lake",
                file_name,
                file_path,
            )
            print("wrote file:", output_file)
        else:
            print("file already exists:", output_file)


if __name__ == "__main__":
    download_twitter_today_files()
