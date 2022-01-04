import sqlite3
import pandas as pd
import os


def load_parquet():
    """
    Load parquet files from local storage
    """
    script_dir = os.path.dirname(os.path.realpath(__file__))
    list_files = os.listdir(f"{script_dir}/twitter_tools/temp_storage/parquet_files")
    df_list = []
    for file_name in list_files:
        df = pd.read_parquet(
            f"{script_dir}/twitter_tools/temp_storage/parquet_files/{file_name}"
        )
        df_list.append(df)
    df = pd.concat(df_list)
    return df


df = load_parquet()
print(df.shape)