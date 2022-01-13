import os
import google_tools.drive_tools as drive_tools
import google_tools.spreadsheet_tools as spreadsheet_tools
import db_tools.sqlite_tools as sqlite_tools
import pandas as pd
import time
import telegram_tools.telegram_tools as telegram_tools
from datetime import datetime
import numpy as np

SHARING_ZONE_DIRECTORY = os.environ.get("SHARING_ZONE_DIRECTORY")
twitter_local_dir = f"{SHARING_ZONE_DIRECTORY}/social_media/twitter"
agents_local_dir = os.scandir(twitter_local_dir)

## Get all folders which are screen_names in local dir
def get_all_local_agents():
    local_agents = []
    for directory in agents_local_dir:
        agent_local_db = f"{directory.name}"
        local_agents.append(agent_local_db)
    return local_agents


## Get directories for the agents local databases
def get_all_local_agents_path(all_agents):
    local_agents_path = []
    for agent in all_agents:
        local_agents_path.append(f"{twitter_local_dir}/{agent}/{agent}.db")
    return local_agents_path


def check_if_all_spreadsheets_exist(all_agents, all_spreadsheets):

    difference = set(all_agents) - set(all_spreadsheets)
    return difference


def create_non_existent_spreadsheets_on_gdrive(new_agent_names):
    for agent in new_agent_names:
        obj_spreadsheet = drive_tools.Create_spreadsheet(agent)
        new_spreadsheet = obj_spreadsheet.create_spreadsheet()
        new_spreadsheet = obj_spreadsheet.upload()
        spreadsheet_tools.add_header_row(new_spreadsheet)
        print("Adding new entry to the database")
        add_new_spreadsheet_data_to_db(new_spreadsheet)


def add_new_spreadsheet_data_to_db(new_agent_name):
    spreadsheet_name = new_agent_name["title"]
    spreadsheet_url = new_agent_name["alternateLink"]
    spreadsheet_folder_id = new_agent_name["parents"][0]["id"]
    sqlite_tools.add_new_spreadsheet(
        spreadsheet_name, spreadsheet_url, spreadsheet_folder_id
    )


def check_posts_to_be_updated(df_agent, df_logs):
    ## Get the agent's posts from the log file
    logged_posts = df_logs["post_id"].values
    ## Create a dataframe with just the posts that are not already in google sheets
    df_output = df_agent[~df_agent["id.long"].isin(logged_posts)]
    ## Just to be sure it is not sending duplicate data
    df_output = df_output.drop_duplicates()
    return df_output


def load_agent_dataframe(agente_db_path):
    df = pd.read_sql(
        f"SELECT * FROM tweets",
        con=sqlite_tools.stand_alone_connection(agente_db_path),
    )
    df["created_at"] = pd.to_datetime(df["created_at"]).dt.tz_localize(None)
    return df


def load_log_dataframe(agent_name):
    df = pd.read_sql(
        f"SELECT post_id FROM spreadsheet_posts WHERE agent_name = '{agent_name}'",
        con=sqlite_tools.stand_alone_connection(
            f"{SHARING_ZONE_DIRECTORY}/social_media/spreadsheets_logs/twitter_logs.db"
        ),
    )
    return df


def write_post_in_logs(post):
    sqlite_tools.insert_new_post(
        post["spreadsheet_id"],
        post["post_id"],
        post["post_created_at"],
        post["spreadsheet_write_date"],
        post["agent_name"],
    )
    return "Done"


def write_post_in_spreadsheet(post, spreadsheet_id):
    post = [post["post_date"], post["text"], post["post_link"]]
    spreadsheet_tools.writer(post, spreadsheet_id)
    return "Done"


def get_data_for_the_spreadsheet_entry(post):
    posts_to_be_made = []
    for index, content in post.iterrows():
        data = {}
        data["post_date"] = content["created_at"].strftime("%Y-%m-%d %H:%M:%S")
        if content["full_text"] != None:
            data["text"] = content["full_text"]
        else:
            data["text"] = content["text"]
        data[
            "post_link"
        ] = f"https://twitter.com/{content['user.screen_name']}/status/{content['id_str']}"
        posts_to_be_made.append(data)
    return posts_to_be_made


all_agents = get_all_local_agents()
all_spreadsheets = drive_tools.list_files_in_dir()
non_existent_spreadsheets = check_if_all_spreadsheets_exist(
    all_agents, all_spreadsheets
)
create_non_existent_spreadsheets_on_gdrive(non_existent_spreadsheets)
all_agents_path = get_all_local_agents_path(all_agents)
for agente_path in set(all_agents_path):
    agent_name = agente_path.split("/")[-1].split(".")[0]
    df_logs = load_log_dataframe(agent_name)
    df_agent = load_agent_dataframe(agente_path)
    post_to_be_updated = check_posts_to_be_updated(df_agent, df_logs)
    print(f"{post_to_be_updated.shape[0]} new posts to be updated from {agent_name}")
    posts = get_data_for_the_spreadsheet_entry(post_to_be_updated)
    for post in posts:
        print(f"writing post from {agent_name}")
        spreadsheet_id = sqlite_tools.get_spreadsheet_id_from_twitter_logs(agent_name)
        write_post_in_spreadsheet(post, spreadsheet_id)
        db_data = {}
        db_data["spreadsheet_id"] = spreadsheet_id
        db_data["post_id"] = post["post_link"].split("/")[-1]
        db_data["post_created_at"] = post["post_date"]
        db_data["spreadsheet_write_date"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        db_data["agent_name"] = agent_name
        write_post_in_logs(db_data)
        print(f"Done writing post from {agent_name} at {datetime.now()}")
