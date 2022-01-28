## This script upload all files in the twitter directory and also log each tweet inside of these files.
## This way will be possible to easily remove tweets requested by users.

import aws_tools.s3_tools as s3_tools
import os
from datetime import datetime
import pytz
import telegram_tools.telegram_tools as telegram_tools
import json
import db_tools.postgres_tools as postgres_tools
import time
import psycopg2

## List all files in the twitter directory
def orchestrator():
    """
    Creates the bucket path, logs all tweets in database and removes the files from the local directory.
    """
    now = datetime.now()
    day = now.day
    if len(str(day)) == 1:
        day = f"0{now.day}"
    month = now.month
    if len(str(month)) == 1:
        month = f"0{now.month}"
    year = now.year
    script_path = os.path.dirname(os.path.realpath(__file__))
    twitter_files_path = f"{script_path}/twitter_tools/temp_storage/compressed_files"
    files = os.listdir(twitter_files_path)
    for file in set(files):
        post_lake_dir = f"social_media/twitter/landing_zone/year={year}/month={month}/day={day}/{file}"
        content = f"{twitter_files_path}/{file}"
        print(f"reading {file}")
        process_file_to_db(content, post_lake_dir)
        file_size = os.path.getsize(content)
        file_size = file_size / 1024 ** 2
        file_size = round(file_size, 2)
        request = s3_tools.upload_compressed_file_from_local_directory(
            "ogb-lake", content, post_lake_dir
        )
        if request == 200:
            remove_files(twitter_files_path, file, file_size)


def remove_files(twitter_files_path, file, file_size):
    """
    Removes the files from the local directory.
    """
    print(f"{file} uploaded successfully")
    print(f"removing {file} from local directory")
    telegram_tools.send_message(
        f"The {file_size} MB file uploaded successfully at {datetime.now()}"
    )
    os.remove(f"{twitter_files_path}/{file}")


def process_file_to_db(file, post_lake_dir):
    """
    Receives a all tweets from multiline file and iterates over them.
    Checks whether the tweet is an ogb agent or not.
    If ogb agent, it will be added to a connected table. If not, it will be added to a non connected table.
    """
    ingestion_datetime = file.split("/")[-1].replace("tweets-", "").replace(".json", "")
    with open(file, "r") as f:
        f_str = f.read().splitlines()
        for line in f_str:
            try:
                row = json.loads(line)
                output = {}
                output["post_platform_id"] = row["id_str"]
                output["post_date"] = row["created_at"]
                output["ingestion_datetime"] = ingestion_datetime
                output["post_lake_dir"] = post_lake_dir
                output["screen_name"] = row["user"]["screen_name"]
                output["agent_platform_id"] = row["user"]["id_str"]
                output["ogb_agent"] = row["ogb_agent"]
                ## A little delay to avoid stressing the cheap database
                time.sleep(0.01)
                write_to_db(output)
            except json.decoder.JSONDecodeError as e:
                print(f"Json error > {e}")
                pass


def get_agent_and_profile_id(agent_platform_id):
    """
    Gets the agent_id and profile_id from the database.
    If no agent is found, it will return None for both agent_id and profile_id.
    """
    output = postgres_tools.free_style_select(
        f"""SELECT agent_id, twitter_profile_id
        FROM twitter_profiles
        WHERE agent_platform_id = '{agent_platform_id}'"""
    )
    if output == None:
        telegram_tools.send_message(
            f"The agent_platform_id {agent_platform_id} does not exist in the database"
        )
        return None
    elif output != None:
        return output


def write_to_db(data):
    """
    Writes the data to the database.
    Checks if ogb_agent is True or False.
    If True it will also check if the agent exists in the database.
    If not, it will send a telegram message and continue"""
    if data["ogb_agent"] == False:
        postgres_tools.add_new_twitter_post_non_agent(
            data["post_platform_id"],
            data["agent_platform_id"],
            data["post_date"],
            data["ingestion_datetime"],
            data["post_lake_dir"],
        )
    elif data["ogb_agent"] == True:
        agent_and_profile_id = get_agent_and_profile_id(data["agent_platform_id"])
        agent_id = agent_and_profile_id[0]
        profile_id = agent_and_profile_id[1]
        if agent_id != None:
            postgres_tools.add_new_twitter_post(
                data["post_platform_id"],
                data["post_date"],
                data["ingestion_datetime"],
                data["post_lake_dir"],
                profile_id,
                agent_id,
            )
        else:
            telegram_tools.send_message(
                f"The agent_platform_id {data['post_platform_id']} {data['screen_name']}does not exist in the database"
            )
            return


if __name__ == "__main__":
    try:
        orchestrator()
    except Exception as e:
        telegram_tools.send_message(f"Uploader failed  {e}")
