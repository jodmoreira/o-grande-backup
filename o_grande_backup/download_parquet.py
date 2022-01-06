## This script download parquet files from the s3 bucket and stores it locally to then be read by another script which
## will load the data into local directories

import aws_tools.s3_tools as s3_tools
import os
from datetime import date


def download_twitter_today_files():
    """
    This function downloads the parquet files from the s3 bucket and stores it locally
    """
    year = date.today().year
    month = date.today().month
    # Because the month directory in the lake uses leading zeros, it is needed to add a 0 if the month is less than 10
    if len(str(month)) == 1:
        month = "0" + str(month)
    day = date.today().day
    # Because the day directory in the lake uses leading zeros, it is needed to add a 0 if the day is less than 10
    if len(str(day)) == 1:
        day = "0" + str(day)
    # Get the script path
    script_dir = os.path.dirname(os.path.realpath(__file__))
    # Lists all S3 files in the directory of the current day
    list_s3_files = s3_tools.list_files(
        "ogb-lake",
        f"social_media/twitter/structured_zone/agent/year=2022/month=01/day=01/",
    )
    # Iterate through the list of file paths and download them
    for file_name in list_s3_files:
        # Get only the last name of the file path, which is the file name itself
        output_file = file_name.split("/")[-1]

        file_path = (
            f"{script_dir}/twitter_tools/temp_storage/parquet_files/{output_file}"
        )
        ## Check if file already exists to avoid write the same file twice
        if not os.path.isfile(file_path):
            # Download the file from the s3 bucket and store in a temporary directory
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
