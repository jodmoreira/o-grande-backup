import pandas as pd
import os
import sqlite3

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
FILES_PATH = f"{SCRIPT_DIR}/twitter_tools/temp_storage/parquet_files"


def list_parquet_files():
    list_files = os.listdir(FILES_PATH)
    return list_files


def create_dataframe(list_files):
    df_list = []
    for file_name in list_files:
        df = pd.read_parquet(f"{FILES_PATH}/{file_name}")
        df_list.append(df)
    df = pd.concat(df_list)
    return df


def get_unique_screen_name(df):
    return df["user.screen_name"].unique()


def check_directories(screen_names):
    for screen_name in screen_names:
        dir_exists = os.path.exists(
            f"/home/pi/sharing_zone/social_media/twitter/{screen_name}"
        )
        if dir_exists == False:
            os.makedirs(f"/home/pi/sharing_zone/social_media/twitter/{screen_name}")


def write_to_db(df, screen_names):
    for screen_name in screen_names:
        df_loop = df[df["user.screen_name"] == screen_name]
        folder_name = df_loop["user.screen_name"].values[0]
        conn = sqlite3.connect(
            f"/home/pi/sharing_zone/social_media/twitter/{folder_name}/{folder_name}.db"
        )
        df_loop.to_sql("tweets", conn, if_exists="append", index=False)
        print(f"wrote {screen_name} to db")


## Delete parquet files
def delete_files(files_list):
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
