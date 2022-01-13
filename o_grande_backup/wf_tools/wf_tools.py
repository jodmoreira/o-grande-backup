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
    response = json.loads(response.text)
    return response


def get_all_sites():
    url = f"{MAIN_URL}/sites"
    headers = {"Authorization": f"Bearer {API_KEY}", "accept-version": "1.0.0"}
    time.sleep(1)
    response = requests.request("GET", url, headers=headers)
    response = json.loads(response.text)
    return response


def get_specific_site(site_id):
    url = f"{MAIN_URL}/sites/{site_id}"
    payload = {}
    headers = {"Authorization": f"Bearer {API_KEY}", "accept-version": "1.0.0"}
    time.sleep(1)
    response = requests.request("GET", url, headers=headers, data=payload)
    response = json.loads(response.text)
    return response


def get_all_collections(site_id):
    url = f"{MAIN_URL}/sites/{site_id}/collections"
    headers = {"Authorization": f"Bearer {API_KEY}", "accept-version": "1.0.0"}
    time.sleep(1)
    response = requests.request("GET", url, headers=headers)
    response = json.loads(response.text)
    return response


def get_collection(site_id, collection_id):
    url = f"{MAIN_URL}/sites/{site_id}/collections/{collection_id}"
    headers = {"Authorization": f"Bearer {API_KEY}", "accept-version": "1.0.0"}
    time.sleep(1)
    response = requests.request("GET", url, headers=headers)
    return response


def get_all_items_from_collections(site_id, collection_id, offset=None):
    if offset is None:
        offset = 0
    url = f"{MAIN_URL}/collections/{collection_id}/items?offset={offset}"
    headers = {"Authorization": f"Bearer {API_KEY}", "accept-version": "1.0.0"}
    time.sleep(1)
    response = requests.request("GET", url, headers=headers)
    response = json.loads(response.text)
    return response


def get_single_item(site_id, collection_id, item_id):
    url = f"{MAIN_URL}/sites/{site_id}/collections/{collection_id}/items/{item_id}"
    headers = {"Authorization": f"Bearer {API_KEY}", "accept-version": "1.0.0"}
    time.sleep(1)
    response = requests.request("GET", url, headers=headers)
    response = json.loads(response.text)
    return response


def create_collection_item(site_id, collection_id, data):
    url = f"{MAIN_URL}/sites/{site_id}/collections/{collection_id}/items"
    payload = json.dumps(data)
    headers = {"Authorization": f"Bearer {API_KEY}", "accept-version": "1.0.0"}
    time.sleep(1)
    response = requests.request("POST", url, headers=headers, data=payload)
    response = json.loads(response.text)
    return response


def update_collection_item(site_id, collection_id, item_id, data):
    """
    Add a collection item
    """
    url = f"{MAIN_URL}/sites/{site_id}/collections/{collection_id}/items/{item_id}"
    payload = json.dumps(data)
    headers = {"Authorization": f"Bearer {API_KEY}", "accept-version": "1.0.0"}
    time.sleep(1)
    response = requests.request("PUT", url, headers=headers, data=payload)
    response = json.loads(response.text)
    return response


def patch_collection_item(collection_id, item_id, field_name, field_value):
    """
    Update a collection item
    """
    url = f"{MAIN_URL}/collections/{collection_id}/items/{item_id}"
    data_to_update = '{"fields": {"%s": "%s"}}' % (
        field_name,
        field_value,
    )
    headers = {
        "Authorization": f"Bearer {API_KEY}",
        "accept-version": "1.0.0",
        "Content-Type": "application/json",
    }
    time.sleep(1)
    response = requests.request("PATCH", url, headers=headers, data=data_to_update)
    response = json.loads(response.text)
    print(response)
    return response


def get_all_agents():
    all_agents = []
    site_id = get_all_sites()[0]["_id"]
    collection_id = get_all_collections(site_id)[0]["_id"]
    agents = get_all_items_from_collections(site_id, collection_id)
    offset = agents["offset"]
    while agents["count"] != 0:
        all_agents.append(agents["items"])
        offset = agents["offset"] + 100
        agents = get_all_items_from_collections(site_id, collection_id, offset)
    output = []
    for i in all_agents:
        for j in i:
            output.append(j)
    return output
