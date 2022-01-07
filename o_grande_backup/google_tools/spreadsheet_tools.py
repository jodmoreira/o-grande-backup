import gspread
from google.oauth2 import service_account
import pandas as pd
from gspread.exceptions import APIError
import time
import os
import requests

CREDENTIALS = os.environ.get("CREDENTIALS")


def login():
    credentials = CREDENTIALS
    credentials = service_account.Credentials.from_service_account_file(credentials)
    scoped_credentials = credentials.with_scopes(
        [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive",
        ]
    )
    gc = gspread.authorize(scoped_credentials)
    return gc


def writer(value, spreadsheet, worksheet="Sheet1"):
    try:
        gc = login()
        spreadsheet = gc.open_by_key(spreadsheet)
        worksheet = spreadsheet.worksheet(worksheet)
        worksheet.append_row(value, value_input_option="USER_ENTERED")
        time.sleep(1)
    except APIError as e:
        print(e)
        time.sleep(60)
        writer(value, spreadsheet, worksheet)
    except requests.exceptions.ConnectionError:
        time.sleep(180)
        writer(value, spreadsheet, worksheet)


def add_header_row(spreadsheet):
    print("Adding the header row")
    spreadsheet = spreadsheet["id"]
    columns = ["Data do Tweet", "Tweet", "Endereço da Publicação"]
    writer(columns, spreadsheet, "Sheet1")
