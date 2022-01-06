from pydrive.auth import GoogleAuth, ServiceAccountCredentials
from pydrive.drive import GoogleDrive
from pydrive.files import ApiRequestError
from datetime import datetime
import os
import time

CREDENTIALS = os.environ.get("CREDENTIALS")
DRIVE_TWITTER_FOLDER_ID = os.environ.get("DRIVE_TWITTER_FOLDER_ID")
MY_EMAIL = os.environ.get("MY_EMAIL")


def login():
    gauth = GoogleAuth()
    scope = ["https://www.googleapis.com/auth/drive"]
    gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(
        CREDENTIALS, scope
    )
    drive = GoogleDrive(gauth)
    return drive


def list_files_in_dir():
    drive = login()
    files = drive.ListFile(
        {"q": f"'{DRIVE_TWITTER_FOLDER_ID}' in parents and trashed=false"}
    ).GetList()
    files = [i["title"] for i in files]
    return files


def create_folder(folder_name, parent_folder_id):
    drive = login()
    folder_metadata = {
        "name": folder_name,
        "parents": [{"id": DRIVE_TWITTER_FOLDER_ID}],
        "mimeType": "application/vnd.google-apps.folder",
    }
    folder = drive.CreateFile(folder_metadata)
    folder.Upload()
    return folder


class Create_spreadsheet:
    def __init__(self, spread_sheetname):
        self.drive = login()
        self.agent = spread_sheetname
        self.spreadsheet_metadata = {
            "title": spread_sheetname,
            "parents": [{"id": DRIVE_TWITTER_FOLDER_ID}],
            "mimeType": "application/vnd.google-apps.spreadsheet",
        }

    def create_spreadsheet(self):
        print(f"Creating spreadsheet for {self.agent}")
        try:
            self.spreadsheet = self.drive.CreateFile(self.spreadsheet_metadata)
            return self.spreadsheet
        except ApiRequestError as e:
            print(e)
            time.sleep(30)
            self.create_spreadsheet()

    def upload(self):
        print(f"Uploading spreadsheet for {self.agent}")
        try:
            self.spreadsheet.Upload()
            return self.spreadsheet
        except ApiRequestError as e:
            print(e)
            time.sleep(30)
            self.uploader()

    def insert_permission(self):
        print(f"Inserting permission for {self.agent}")
        try:
            self.spreadsheet.InsertPermission(
                {"value": MY_EMAIL, "type": "user", "role": "owner"}
            )
            return self.spreadsheet
        except ApiRequestError as e:
            print(e)
            time.sleep(30)
            self.insert_permission()


def create_spreadsheet(spread_sheetname):
    drive = login()
    spreadsheet_metadata = {
        "title": spread_sheetname,
        "parents": [{"id": DRIVE_TWITTER_FOLDER_ID}],
        "mimeType": "application/vnd.google-apps.spreadsheet",
    }
    spreadsheet = drive.CreateFile(spreadsheet_metadata)
    print("Spreadsheet upload process")
    spreadsheet.Upload()
    # Insert the permission.
    print("Spreadsheet inserting permission")
    permission = spreadsheet.InsertPermission(
        {"value": MY_EMAIL, "type": "user", "role": "owner"}
    )
    return spreadsheet
