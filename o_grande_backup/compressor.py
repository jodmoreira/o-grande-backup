import os, tarfile
from datetime import datetime
import pytz
import json
import telegram_tools.telegram_tools as telegram_tools

## Add directory files to a single tar file

##get script directory


class Compressor_tools:
    def __init__(self, json_files_path):
        self.script_path = os.path.dirname(os.path.realpath(__file__))
        self.now = datetime.now()
        self.bsb_tz = pytz.timezone("America/Sao_Paulo")
        self.ingestion_datetime = self.bsb_tz.localize(self.now).strftime(
            "%Y-%m-%d %H:%M:%S%z"
        )
        self.json_files_path = f"{self.script_path}/{json_files_path}"
        self.json_multiline_path = f"{self.script_path}/twitter_tools/temp_storage/compressed_files/tweets-{self.ingestion_datetime}.json"
        self.json_files = os.listdir(self.json_files_path)
        self.files_count = len(self.json_files)

    def create_json_file(self):
        with open(self.json_multiline_path, "a+") as new_json_file:
            for i in self.json_files:
                data = open(
                    f"{self.script_path}/twitter_tools/temp_storage/json_data/{i}", "r"
                )
                new_json_file.write(data.read())
                new_json_file.write("\n")

    def list_json_multiline_ids(self):
        with open(self.json_multiline_path, "r") as json_multiline:
            json_multiline = json_multiline.read().splitlines()
            json_ids = [str(json.loads(i)["id"]) for i in json_multiline]
        return json_ids

    def list_json_files_id(self):
        json_files_id = [str(i.split("-")[1].split("__")[0]) for i in self.json_files]
        return json_files_id

    def check_intersection_files(self):
        json_multilines = self.list_json_multiline_ids()
        directory_files = self.list_json_files_id()
        intersection = set(json_multilines).intersection(directory_files)
        return intersection

    def delete_intersection_files(self):
        intersection_ids = self.check_intersection_files()
        for intersection_id in intersection_ids:
            for file in self.json_files:
                if intersection_id in file:
                    os.remove(f"{self.json_files_path}{file}")


def routine():
    json_files_path = "twitter_tools/temp_storage/json_data/"
    compressor = Compressor_tools(json_files_path)
    json_multiline_path_name = compressor.json_multiline_path.split("/")[-1]
    print(f"Adding files to a json multiline named {json_multiline_path_name}")
    compressor.create_json_file()
    compressor.delete_intersection_files()
    telegram_tools.send_message(
        f"Compressed files created: {json_multiline_path_name} from {compressor.files_count} files"
    )


if __name__ == "__main__":
    routine()
