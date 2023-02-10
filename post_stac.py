import requests
import json
import sys
import os
import shutil
import zipfile
import time
from pathlib import Path
from urllib.parse import urljoin
from requests.auth import HTTPBasicAuth

workingdir = Path(__file__).parent
sentinel_data = (workingdir / "Sentinel2-tileless" / "sentinel2_full_test")

app_host = "http://86.50.229.158:8082/"

def post_or_put(url: str, data: dict):
    """Post or put data to url."""
    r = requests.post(url, json=data)
    if r.status_code == 409:
        new_url = url if data["type"] == "Collection" else url + f"/{data['id']}"
        # Exists, so update
        r = requests.put(new_url, json=data)
        # Unchanged may throw a 404
        if not r.status_code == 404:
            r.raise_for_status()
    else:
        r.raise_for_status()


def ingest_sentinel_data(app_host: str = app_host, data_dir: Path = sentinel_data):

    with open(data_dir / "collection.json") as f:
        rootcollection = json.load(f)

    # post_or_put(urljoin(app_host, "/collections"), rootcollection)
    # print("Collection POSTed")

    items = [x['href'] for x in rootcollection["links"] if x["rel"] == "item"]

    print("POSTing items: ", end='')

    for i, item in enumerate(items):
        if i < (len(items) / 2):
            continue
            with open(data_dir / item) as f:
                payload = json.load(f)
                post_or_put(urljoin(app_host, f"collections/{rootcollection['id']}/items"), payload)
                print("/", end='', flush=True)
        else:
            # continue
            with open(data_dir / item) as f:
                payload = json.load(f)
                post_or_put(urljoin(app_host, f"collections/{rootcollection['id']}/items"), payload)
                print("/", end='', flush=True)
    
    print("",flush=True)

if __name__ == "__main__":

    ingest_sentinel_data()