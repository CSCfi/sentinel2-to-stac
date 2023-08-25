import json
import getpass
from pathlib import Path
import requests
import pystac_client
from requests.auth import HTTPBasicAuth
from urllib.parse import urljoin

def json_convert(jsonfile):

    """
        jsonfile: json file in dict format
        
        A function to map the Sentinel-2 STAC jsonfiles into the GeoServer database layout.
        There are different json layouts for Collections and Items. The function checks if the jsonfile is of type "Collection",
        or of type "Feature" (=Item). A number of properties are hardcoded into Sentinel-2 metadata as these are not collected in the STAC jsonfiles.
    """

    with open(jsonfile) as f:
        content = json.load(f)
    
    if content["type"] == "Collection":

        new_json = {
            "type": "Feature",
            "geometry": {
                "type": "Polygon",
                "coordinates": [
                    [
                        [
                            content["extent"]["spatial"]["bbox"][0][2],
                            content["extent"]["spatial"]["bbox"][0][1]
                        ],
                        [
                            content["extent"]["spatial"]["bbox"][0][2],
                            content["extent"]["spatial"]["bbox"][0][3]
                        ],
                        [
                            content["extent"]["spatial"]["bbox"][0][0],
                            content["extent"]["spatial"]["bbox"][0][3]
                        ],
                        [
                            content["extent"]["spatial"]["bbox"][0][0],
                            content["extent"]["spatial"]["bbox"][0][1]
                        ],
                        [
                            content["extent"]["spatial"]["bbox"][0][2],
                            content["extent"]["spatial"]["bbox"][0][1]
                        ]

                    ]
                ]
            },
            "properties": {
                "name": content["id"],
                "title": content["title"],
                "eo:identifier": content["id"],
                "description": content["description"],
                "timeStart": content["extent"]["temporal"]["interval"][0][0],
                "timeEnd": content["extent"]["temporal"]["interval"][0][1],
                "primary": True,
                "license": content["license"],
                "providers": content["providers"],
                "assets": content["assets"],
                "licenseLink": None,
                "summaries": content["summaries"],
                "queryables": [
                    "eo:identifier",
                    "eo:cloud_cover"
                ]
            }
        }

        if "assets" in content:
            new_json["properties"]["assets"] = content["assets"]

        for link in content["links"]:
            if link["rel"] == "license":
                new_json["properties"]["licenseLink"] = {
                    "href": link["href"],
                    "rel": "license",
                    "type": "application/json"
                } # New License URL link

    if content["type"] == "Feature":

        new_json = {
            "type": "Feature",
            "geometry": content["geometry"],
            "properties": {
                "eop:identifier": content["id"],
                "eop:parentIdentifier": content["collection"],
                "timeStart": content["properties"]["datetime"],
                "timeEnd": content["properties"]["datetime"],
                "opt:cloudCover": int(content["properties"]["eo:cloud_cover"]),
                "crs": content["properties"]["proj:epsg"],
                "projTransform": content["proj:transform"],
                "thumbnailURL": content["assets"]["thumbnail"]["href"],
                "assets": content["assets"]
            }
        }

    return json.loads(json.dumps(new_json))

if __name__ == "__main__":

    pwd = getpass.getpass()

    workingdir = Path(__file__).parent
    sentinel = workingdir / "Sentinel2-tileless" / "sentinel2-l2a"

    app_host = "https://paituli.csc.fi/geoserver/rest/oseo/"
    catalog = pystac_client.Client.open("https://paituli.csc.fi/geoserver/ogc/stac/v1/")

    # Convert the STAC collection json into json that GeoServer can handle
    converted = json_convert(sentinel / "collection.json")

    # Additional code for changing collection data if the collection already exists
    collections = catalog.get_collections()
    collection_ids = [collection.id for collection in collections]
    if "sentinel2-l2a" in collection_ids:
        r = requests.put(urljoin(app_host, f"collections/sentinel2-l2a"), json=converted, auth=HTTPBasicAuth("admin", pwd))
        r.raise_for_status()
    else:
        r = requests.post(urljoin(app_host, "collections/"), json=converted, auth=HTTPBasicAuth("admin", pwd))
        r.raise_for_status()

    # Get the posted items from the specific collection
    posted = catalog.search(collections=["sentinel2-l2a"]).item_collection()
    posted_ids = [x.id for x in posted]
    print(f"POSTed: {len(posted_ids)}")

    with open(sentinel / "collection.json") as f:
        rootcollection = json.load(f)

    items = [x['href'] for x in rootcollection["links"] if x["rel"] == "item"]

    print("POSTing items:")
    for i, item in enumerate(items):
        with open(sentinel / item) as f:
            payload = json.load(f)
        # Convert the STAC item json into json that GeoServer can handle
        converted = json_convert(sentinel / item)
        request_point = f"collections/{rootcollection['id']}/products"
        if payload["id"] in posted_ids:
            request_point = f"collections/{rootcollection['id']}/products/{payload['id']}"
            r = requests.put(urljoin(app_host, request_point), json=converted, auth=HTTPBasicAuth("admin", pwd))
            r.raise_for_status()
        else:
            r = requests.post(urljoin(app_host, request_point), json=converted, auth=HTTPBasicAuth("admin", pwd))
            r.raise_for_status()
        print("*", end='', flush=True) # Just to keep track that the script is still running