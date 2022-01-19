## Reads all parquet files downloaded to the temp storage directory and writes them to the database of each agent


import pandas as pd
import os
import sqlite3

# Get the current directory of the script
SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
# Generate the path which the parquet files are stored
FILES_PATH = f"{SCRIPT_DIR}/twitter_tools/temp_storage/parquet_files"
SHARING_ZONE_DIRECTORY = os.environ["SHARING_ZONE_DIRECTORY"]


def list_parquet_files():
    """
    Returns a list of all the parquet files in the temp_storage directory
    """
    list_files = os.listdir(FILES_PATH)
    return list_files


def create_dataframe(list_files):
    """
    Creates a single dataframe from all the parquet files
    """
    df_list = []
    for file_name in list_files:
        df = pd.read_parquet(f"{FILES_PATH}/{file_name}")
        df_list.append(df)
    df = pd.concat(df_list)
    try:
        df["id.long"] = df["id"]
        df = df.drop(columns=["id"])
    except:
        pass
    return df


def get_unique_screen_name(df):
    """
    Gets the unique screen names from the dataframe created from the parquet files
    """
    return df["user.screen_name"].unique()


def check_directories(screen_names):
    for screen_name in screen_names:
        dir_exists = os.path.exists(
            f"{SHARING_ZONE_DIRECTORY}/social_media/twitter/{screen_name}"
        )
        if dir_exists == False:
            os.makedirs(f"{SHARING_ZONE_DIRECTORY}/social_media/twitter/{screen_name}")


def write_to_db(df, screen_names):
    for screen_name in screen_names:
        df_loop = df[df["user.screen_name"] == screen_name]
        folder_name = df_loop["user.screen_name"].values[0]
        print(f"writing {screen_name} to db")
        conn = sqlite3.connect(
            f"{SHARING_ZONE_DIRECTORY}/social_media/twitter/{folder_name}/{folder_name}.db"
        )
        try:
            df_loop.to_sql("tweets", conn, if_exists="append", index=False)
        except sqlite3.OperationalError:
            existing_table = pd.read_sql_query("SELECT * FROM tweets", conn)
            df_concatenate = pd.concat([existing_table, df_loop])
            df_concatenate.to_sql("tweets", conn, if_exists="replace", index=False)
        conn.close()


def delete_files(files_list):
    """
    Delete the parquet files from the temp_storage directory
    """
    for file_name in files_list:
        os.remove(f"{FILES_PATH}/{file_name}")


if __name__ == "__main__":
    print("Listing Parquet")
    list_files = list_parquet_files()
    print("Creating Dataframe")
    df = create_dataframe(list_files)
    screen_names = get_unique_screen_name(df)
    check_directories(screen_names)
    write_to_db(df, screen_names)
    delete_files(list_files)
