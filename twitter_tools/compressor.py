import os, tarfile
from datetime import datetime
import pytz

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
        self.compressed_files_path = f"{self.script_path}/temp_storage/compressed_files/tweets-{self.ingestion_datetime}.tar.gz"

    def compress_files_to_tar(self):
        with tarfile.open(self.compressed_files_path, "w:gz") as tar:
            tar.add(
                self.json_files_path, arcname=os.path.basename(self.json_files_path)
            )

    def list_compressed_files(self):
        with tarfile.open(self.compressed_files_path) as tar:
            compressed_files = tar.getmembers()
            compressed_files_names = [i.name for i in compressed_files]
        return compressed_files_names

    def list_json_files(self):
        json_files = os.listdir(self.json_files_path)
        return json_files

    def check_intersection_files(self):
        compresse_files = self.list_compressed_files()
        directory_files = self.list_json_files()
        intersection = set(compresse_files).intersection(directory_files)
        return intersection

    def delete_intersection_files(self):
        intersection = self.check_intersection_files()
        for file in intersection:
            os.remove(f"{self.json_files_path}{file}")


def routine():
    json_files_path = "temp_storage/json_data/"
    compressor = Compressor_tools(json_files_path)
    print(f"Compressing files to {compressor.compressed_files_path}")
    compressor.compress_files_to_tar()
    print(f"Deleting files from {compressor.json_files_path}")
    compressor.delete_intersection_files()


if __name__ == "__main__":
    routine()
