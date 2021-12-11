import os
import requests
import json
import time

API_KEY = os.environ.get("WFAPI")
MAIN_URL = "https://api.webflow.com"


def authorization_info():
    url = f"{MAIN_URL}/info"
    payload = {}
    headers = {"Authorization": f"Bearer {API_KEY}", "accept-version": "1.0.0"}
    time.sleep(1)
    response = requests.request("GET", url, headers=headers, data=payload)

    return response.text


def get_all_sites():
    url = f"{MAIN_URL}/sites"
    headers = {"Authorization": f"Bearer {API_KEY}", "accept-version": "1.0.0"}
    time.sleep(1)
    response = requests.request("GET", url, headers=headers)

    return response.text


def get_specific_site(site_id):
    url = f"{MAIN_URL}/sites/{site_id}"
    payload = {}
    headers = {"Authorization": f"Bearer {API_KEY}", "accept-version": "1.0.0"}
    time.sleep(1)
    response = requests.request("GET", url, headers=headers, data=payload)

    return response.text


def get_all_collections(site_id):
    url = f"{MAIN_URL}/sites/{site_id}/collections"
    headers = {"Authorization": f"Bearer {API_KEY}", "accept-version": "1.0.0"}
    time.sleep(1)
    response = requests.request("GET", url, headers=headers)

    return response.text


def get_collection(site_id, collection_id):
    url = f"{MAIN_URL}/sites/{site_id}/collections/{collection_id}"
    headers = {"Authorization": f"Bearer {API_KEY}", "accept-version": "1.0.0"}
    time.sleep(1)
    response = requests.request("GET", url, headers=headers)

    return response.text


def get_all_items_from_collections(site_id, collection_id, offset=0):
    url = f"{MAIN_URL}/collections/{collection_id}/items?offset={offset}"
    headers = {"Authorization": f"Bearer {API_KEY}", "accept-version": "1.0.0"}
    time.sleep(1)
    response = requests.request("GET", url, headers=headers)

    return response.text


def get_single_item(site_id, collection_id, item_id):
    url = f"{MAIN_URL}/sites/{site_id}/collections/{collection_id}/items/{item_id}"
    headers = {"Authorization": f"Bearer {API_KEY}", "accept-version": "1.0.0"}
    time.sleep(1)
    response = requests.request("GET", url, headers=headers)

    return response.text


def create_collection_item(site_id, collection_id, data):
    url = f"{MAIN_URL}/sites/{site_id}/collections/{collection_id}/items"
    payload = json.dumps(data)
    headers = {"Authorization": f"Bearer {API_KEY}", "accept-version": "1.0.0"}
    time.sleep(1)
    response = requests.request("POST", url, headers=headers, data=payload)

    return response.text


def update_collection_item(site_id, collection_id, item_id, data):
    url = f"{MAIN_URL}/sites/{site_id}/collections/{collection_id}/items/{item_id}"
    payload = json.dumps(data)
    headers = {"Authorization": f"Bearer {API_KEY}", "accept-version": "1.0.0"}
    time.sleep(1)
    response = requests.request("PUT", url, headers=headers, data=payload)

    return response.text


def patch_collection_item(site_id, collection_id, item_id):
    url = f"{MAIN_URL}/sites/{site_id}/collections/{collection_id}/items/{item_id}"
    payload = json.dumps(data)
    headers = {"Authorization": f"Bearer {API_KEY}", "accept-version": "1.0.0"}
    time.sleep(1)
    response = requests.request("PATCH", url, headers=headers, data=payload)

    return response.text
