import boto3
import pystac
import rasterio
import re
import pandas as pd
import getpass
import argparse
import requests
import pystac_client
import time
from urllib.parse import urljoin
from itertools import chain
from datetime import datetime
from xml.dom import minidom
from shapely.geometry import box, mapping
from pystac.extensions.eo import EOExtension, Band
from pystac.extensions.projection import ProjectionExtension
from rasterio.warp import transform_bounds
from rasterio.crs import CRS

def change_to_https(request: requests.Request) -> requests.Request: 
    request.url = request.url.replace("http:", "https:")
    # This is to help filtering logging, not needed otherwise
    request.headers["User-Agent"] = "update-script"
    return request

def init_client():

    # Create client with credentials. Allas-conf needed to be run for boto3 to get the credentials
    s3_client = boto3.client(
        service_name = "s3",
        endpoint_url = "https://a3s.fi", 
        region_name = "regionOne"
    )

    return s3_client

def get_buckets(client):
    """
        client: boto3.client
    """
    # Get Buckets from the Maria CSC project   
    bucket_information = client.list_buckets()
    buckets = [x['Name'] for x in bucket_information['Buckets'] if re.match(r"Sentinel2(?!.*segments)", x['Name'])]

    # Get Buckets from these two CSC projects
    first_csv = pd.read_table("2000290_buckets.csv", header=None)
    first_buckets = list(chain.from_iterable(first_csv.to_numpy()))
    second_csv = pd.read_table("2001106_buckets.csv", header=None)
    second_buckets = list(chain.from_iterable(second_csv.to_numpy()))
    
    buckets = [*buckets, *first_buckets, *second_buckets]

    return buckets

def json_convert(content):

    """ 
    A function to map the STAC dictionaries into the GeoServer database layout.
    There are different json layouts for Collections and Items. The function checks if the dictionary is of type "Collection",
    or of type "Feature" (=Item).

    content - STAC dictionary from where the modified JSON will be made
    """
    
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
                # "providers": content["providers"],
                "licenseLink": None,
                # "summaries": content["summaries"],
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
                new_json["properties"]["licenseLink"] = { #New License URL link
                    "href": link["href"],
                    "rel": "license",
                    "type": "application/json"
                }
            elif link["rel"] == "derived_from":
                derived_href = link["href"]
                new_json["properties"]["derivedFrom"] = {
                    "href": derived_href,
                    "rel": "derived_from",
                    "type": "application/json"
                }

    if content["type"] == "Feature":

        new_json = {
            "type": "Feature",
            "geometry": content["geometry"],
            "properties": {
                "eop:identifier": content["id"],
                "eop:parentIdentifier": content["collection"],
                "timeStart": content["properties"]["datetime"],
                "timeEnd": content["properties"]["datetime"],
                # "eop:resolution": content["gsd"],
                "opt:cloudCover": int(content["properties"]["eo:cloud_cover"]),
                "crs": content["properties"]["proj:epsg"],
                # "projTransform": content["properties"]["proj:transform"],
                # "thumbnailURL": content["links"]["thumbnail"]["href"],
                "assets": content["assets"]
            }
        }

    return new_json

def get_xml_content(doc, tagname):

    """
        doc: Parsed xml metadata file
        tagname: The wanted tag to be searched from the xml file
    """

    content = doc.getElementsByTagName(tagname)[0].firstChild.data
    return content

def get_metadata_content(bucket, metadatafile, client):

    """
        bucket: The bucket where the metadatafile is located
        metadatafile: The name of the metadatafile
        client: boto3.client
    """

    obj = client.get_object(Bucket = bucket, Key = metadatafile)['Body']
    metadatacontent = obj.read().decode()
    return metadatacontent

def get_metadata_from_xml(metadatabody):

    """
        metadatabody: The metadata content from boto3.client get_object call
    """

    with minidom.parseString(str(metadatabody)) as doc:
        metadatadict = {}
        metadatadict['cc_perc'] = int(float(get_xml_content(doc,'Cloud_Coverage_Assessment')))
        metadatadict['data_cover'] = 100 - int(float(get_xml_content(doc,'NODATA_PIXEL_PERCENTAGE')))
        metadatadict['start_time'] = get_xml_content(doc,'PRODUCT_START_TIME')
        metadatadict['end_time'] = get_xml_content(doc,'PRODUCT_STOP_TIME')
        metadatadict['orbit'] = get_xml_content(doc,'SENSING_ORBIT_NUMBER')
        metadatadict['baseline'] = get_xml_content(doc,'PROCESSING_BASELINE')

    return metadatadict

def transform_crs(bounds, crs_string):
    
    """
        bounds: Bounding Box bounds from rasterio.open()
        crs_string: CRS string from CRS metadata
    """

    # Transform the bounds according to the CRS
    crs = CRS.from_epsg(4326)
    safecrs = CRS.from_epsg(int(crs_string))
    bounds_transformed = transform_bounds(safecrs, crs, bounds[0][0], bounds[0][1], bounds[0][2], bounds[0][3])
        
    return bounds_transformed

def get_crs(crsmetadatafile):

    """
        crsmetadatafile: The decoded content from the SAFEs CRS metadatafile
    """

    # Get CRS and resolution sizes from crsmetadatafile
    with minidom.parseString(crsmetadatafile) as doc:
        crsstring = get_xml_content(doc, 'HORIZONTAL_CS_CODE').split(':')[-1]
        sizes = doc.getElementsByTagName('Size')
        crsmetadata = { 
            'CRS': crsstring,
            'shapes': {}
        }
        for size in sizes:
            resolution = size.getAttribute('resolution')
            crsmetadata['shapes'][resolution] = (int(get_xml_content(size, 'NROWS')), int(get_xml_content(size, 'NCOLS')))

    return crsmetadata

def make_item(uri, metadatacontent, crs_metadata):
    """
        uri: The SAFE ID of the item (currently URL of the image, could be changes to SAFE later)
        metadatacontent: Metadata dict got from get_metadata_content()
        crs_metadata: CRS metadata dict containing CRS string and shapes for different resolutions from get_crs()
    """
    params = {}

    if re.match(r".+?\d{4}/S2(A|B)", uri):
        params['id'] = uri.split("/")[5].split('.')[0]
    else:
        params['id'] = uri.split('/')[4].split('.')[0]
    
    with rasterio.open(uri) as src:
        item_transform = src.transform
        # as lat,lon
        params['bbox'] = transform_crs(list([src.bounds]),crs_metadata['CRS'])
        params['geometry'] = mapping(box(*params['bbox']))
            
    mtddict = get_metadata_from_xml(metadatacontent)

    # Datetime from filename
    params['datetime'] = datetime.strptime(uri.split('_')[2][0:8], '%Y%m%d')

    params['properties'] = {}
    params['properties']['eo:cloud_cover'] = mtddict['cc_perc']
    #following are not part of eo extension
    params['properties']['data_cover'] = mtddict['data_cover']
    params['properties']['orbit'] = mtddict['orbit']
    params['properties']['baseline'] = mtddict['baseline']
    # following are part of general metadata hardcoded for Sentinel-2
    params['properties']['platform'] = 'sentinel-2'
    params['properties']['instrument'] = 'msi'
    params['properties']['constellation'] = 'sentinel-2'
    params['properties']['mission'] = 'copernicus'
    params['properties']['proj:epsg'] = int(crs_metadata['CRS'])
    params['properties']['gsd'] = 10

    stacItem = pystac.Item(**params)

    # Adding the EO and Projecting Extensions to the item
    eo_ext = EOExtension.ext(stacItem, add_if_missing=True)
    eo_ext.bands = [s2_bands[band]['band'] for band in s2_bands]
    proj_ext = ProjectionExtension.ext(stacItem, add_if_missing=True)
    proj_ext.apply(epsg = int(crs_metadata['CRS']), transform = item_transform)

    return stacItem

def add_asset(stacItem, uri, crsmetadata=None, thumbnail=False):

    """ 
        Adds an asset to the STAC Item based on whether the asset is a thumbnail or an image. 
        stacItem: stac.Item object
        uri: Image URL
        crsmetadata: CRS metadata dict containing CRS string and shapes for different resolutions from get_crs()
        thumbnail: Boolean value indicating if the asset is a thumbnail or not
    """

    if uri.endswith('geo.jp2'): # A few special cases where there were differently named image files that contained different metadata
        splitter = uri.split('/')[-1].split('.')[0].split('_')
        full_bandname = '_'.join(splitter[-3:-1])
        band = splitter[-3]
        resolution = splitter[-2].split('m')[0]
        asset = pystac.Asset(
            href=uri,
            title=full_bandname,
            media_type=pystac.MediaType.JPEG2000,
            roles=["data"],
            extra_fields= {
                'gsd': int(resolution),
                'proj:shape': crsmetadata['shapes'][resolution],
            }
        )
        if band in s2_bands:
            asset_eo_ext = EOExtension.ext(asset)
            asset_eo_ext.bands = [s2_bands[band]["band"]]
        stacItem.add_asset(
            key=full_bandname,
            asset=asset
        )
        
        return stacItem

    if not thumbnail: # If the asset is a standard image
        splitter = uri.split('/')[-1].split('.')[0].split('_')
        full_bandname = '_'.join(splitter[-2:])
        band = splitter[-2]
        resolution = splitter[-1].split('m')[0]
        asset = pystac.Asset(
                href=uri,
                title=full_bandname,
                media_type=pystac.MediaType.JPEG2000,
                roles=["data"],
                extra_fields= {
                    'gsd': int(resolution),
                    'proj:shape': crsmetadata['shapes'][resolution],
                }
        )
        if band in s2_bands:
            asset_eo_ext = EOExtension.ext(asset)
            asset_eo_ext.bands = [s2_bands[band]["band"]]
        stacItem.add_asset(
            key=full_bandname, 
            asset=asset
        )

    else: # If the asset is a thumbnail image
        with rasterio.open(uri) as src:
            shape = src.shape

        full_bandname = uri.split('/')[-1].split('_')[-1].split('.')[0]
        asset = pystac.Asset(
                href=uri,
                title="Thumbnail image",
                media_type=pystac.MediaType.JPEG2000,
                roles=["thumbnail"],
                extra_fields= {
                    'proj:shape': shape,
                }
        )
        stacItem.add_asset(
            key="thumbnail", 
            asset=asset
        )

    return stacItem

def update_catalog(app_host, csc_collection):

    s3_client = init_client()
    buckets = get_buckets(s3_client)
    session = requests.Session()
    session.auth = ("admin", pwd)
    log_headers = {"User-Agent": "update-script"} # Added for easy log-filtering
    original_csc_collection_ids = {item.id for item in csc_collection.get_all_items()}
    print(" * CSC Items collected.")
    items_to_add = {}

    for bucket in buckets:

        # Usual list_objects_v2 function only lists up to 1000 objects so pagination is needed when using a client
        paginator = s3_client.get_paginator('list_objects_v2')
        pages = paginator.paginate(Bucket=bucket)
        bucketcontents = [x['Key'] for page in pages for x in page['Contents']]

        # Gather needed contents into different lists containing filenames
        bucketcontent_jp2 = [x for x in bucketcontents if x.endswith('jp2')]
        bucketcontent_mtd = [x for x in bucketcontents if x.endswith('MTD_MSIL2A.xml')]
        bucketcontent_crs = [x for x in bucketcontents if x.endswith('MTD_TL.xml')]

        exclude = {'index.html'}
        listofsafes = list(set(list(map(lambda x: x.split('/')[0], bucketcontents))) - exclude)
        # One project includes pseudofolders in the path representing the years, with this check, get the actual SAFEs instead
        if any(re.match(r"\d{4}", safe) for safe in listofsafes):
            listofsafes.clear()
            listofsafes = list(set(list(map(lambda x: x.split('/')[1], bucketcontents))) - exclude)
        #print('Bucket:', bucket)
        #print('SAFES:', listofsafes)

        for safe in listofsafes:
            
            # SAFE-filename without the subfix
            safename = str(safe.split('.')[0])

            # IF safename is in Collection, the items are already added
            if safename in original_csc_collection_ids:
                continue

            metadatafile = ''.join((x for x in bucketcontent_mtd if safename in x))
            crsmetadatafile = ''.join((x for x in bucketcontent_crs if safename in x))
            if not metadatafile or not crsmetadatafile:
                # If there is no metadatafile or CRS-metadatafile, the SAFE does not include data relevant to the script
                continue
            # THIS FAILS WITH FOLDER BUCKETS
            safecrs_metadata = get_crs(get_metadata_content(bucket, crsmetadatafile, s3_client))
            
            # only jp2 that are image bands
            jp2images = [x for x in bucketcontent_jp2 if safename in x and 'IMG_DATA' in x]
            # if there are no jp2 imagefiles in the bucket, continue to the next bucket
            if not jp2images:
                continue

            # jp2 that are preview images
            previewimage = next(x for x in bucketcontent_jp2 if safename in x and 'PVI' in x)
            metadatacontent = get_metadata_content(bucket, metadatafile, s3_client)
            
            for image in jp2images:

                uri = 'https://a3s.fi/' + bucket + '/' + image

                # Get the item if it's added during the update, if None, the item is made and preview image added
                if safename not in items_to_add:
                    item = make_item(uri, metadatacontent, safecrs_metadata)
                    items_to_add[safename] = item
                    csc_collection.add_item(item)
                    add_asset(item, 'https://a3s.fi/' + bucket + '/' + previewimage, None, True)
                else:
                    item = items_to_add[safename]
                    add_asset(item, uri, safecrs_metadata)

    for item in items_to_add:
        item_dict = items_to_add[item].to_dict()
        converted_item = json_convert(item_dict)
        request_point = f"collections/{csc_collection.id}/products"
        r = session.post(urljoin(app_host, request_point), headers=log_headers, json=converted_item)
        r.raise_for_status()
    
    if items_to_add:
        print(f" + Number of items added: {len(items_to_add)}")
        # Update the extents from the Allas Items
        csc_collection.update_extent_from_items()
        collection_dict = csc_collection.to_dict()
        converted_collection = json_convert(collection_dict)
        request_point = f"collections/{csc_collection.id}/"

        r = session.put(urljoin(app_host, request_point), headers=log_headers, json=converted_collection)
        r.raise_for_status()
        print(" + Updated Collection Extents.")
    else:
        print(" * All items present.")

if __name__ == "__main__":

    """
    The first check for REST API password is from a password file. 
    If a password file is not found, the script prompts the user to give a password through CLI
    """

    # Band information in Band objects and as a dict
    s2_bands = {
        "B01": {
            "band": Band.create(name='B01', description='Coastal: 400 - 450 nm', common_name='coastal')
        },
        "B02": {
            "band": Band.create(name='B02', description='Blue: 450 - 500 nm', common_name='blue')
        },
        "B03": {
            "band": Band.create(name='B03', description='Green: 500 - 600 nm', common_name='green'),
        },
        "B04": {
            "band": Band.create(name='B04', description='Red: 600 - 700 nm', common_name='red'),
        },
        "B05": {
            "band": Band.create(name='B05', description='Vegetation Red Edge: 705 nm', common_name='rededge')
        },
        "B06": {
            "band": Band.create(name='B06', description='Vegetation Red Edge: 740 nm', common_name='rededge')
        },
        "B07": {
            "band": Band.create(name='B07', description='Vegetation Red Edge: 783 nm', common_name='rededge')
        },
        "B08": {
            "band": Band.create(name='B08', description='Near-IR: 750 - 1000 nm', common_name='nir')
        },
        "B8A": {
            "band": Band.create(name='B8A', description='Near-IR: 750 - 900 nm', common_name='nir08')
        },
        "B09": {
            "band": Band.create(name='B09', description='Water vapour: 850 - 1050 nm', common_name='nir09')
        },
        "B10": {
            "band": Band.create(name='B10', description='SWIR-Cirrus: 1350 - 1400 nm', common_name='cirrus')
        },
        "B11": {
            "band": Band.create(name='B11', description='SWIR16: 1550 - 1750 nm', common_name='swir16')
        },
        "B12": {
            "band": Band.create(name='B12', description='SWIR22: 2100 - 2300 nm', common_name='swir22')
        }
    }

    s2_bands_as_dict = {
        "B01": {
            'name': 'B01', 
            'description': 'Coastal: 400 - 450 nm', 
            'common_name': 'coastal'
        },
        "B02": {
            'name': 'B02', 
            'description': 'Blue: 450 - 500 nm', 
            'common_name': 'blue'
        },
        "B03": {
            'name': 'B03', 
            'description': 'Green: 500 - 600 nm', 
            'common_name': 'green'
        },
        "B04": {
            'name': 'B04', 
            'description': 'Red: 600 - 700 nm', 
            'common_name': 'red'
        },
        "B05": {
            'name': 'B05', 
            'description': 'Vegetation Red Edge: 705 nm', 
            'common_name': 'rededge'
        },
        "B06": {
            'name': 'B06', 
            'description': 'Vegetation Red Edge: 740 nm', 
            'common_name': 'rededge'
        },
        "B07": {
            'name': 'B07', 
            'description': 'Vegetation Red Edge: 783 nm',
            'common_name': 'rededge'
        },
        "B08": {
            'name': 'B08', 
            'description': 'Near-IR: 750 - 1000 nm',
            'common_name': 'nir'
        },
        "B8A": {
            'name': 'B8A', 
            'description': 'Near-IR: 750 - 900 nm',
            'common_name': 'nir08'
        },
        "B09": {
            'name': 'B09', 
            'description': 'Water vapour: 850 - 1050 nm',
            'common_name': 'nir09'
        },
        "B10": {
            'name': 'B10', 
            'description': 'SWIR-Cirrus: 1350 - 1400 nm',
            'common_name': 'cirrus'
        },
        "B11": {
            'name': 'B11', 
            'description': 'SWIR16: 1550 - 1750 nm',
            'common_name': 'swir16'
        },
        "B12": {
            'name': 'B12', 
            'description': 'SWIR22: 2100 - 2300 nm',
            'common_name': 'swir22'
        }
    }

    pw_filename = 'passwords.txt'
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", type=str, help="Hostname of the selected STAC API", required=True)
    
    args = parser.parse_args()

    try:
        pw_file = pd.read_csv(pw_filename, header=None)
        pwd = pw_file.at[0,0]
    except FileNotFoundError:
        print("Password not given as an argument and no password file found")
        pwd = getpass.getpass()
    
    start = time.time()

    app_host = f"{args.host}/geoserver/rest/oseo/"
    csc_catalog = pystac_client.Client.open(f"{args.host}/geoserver/ogc/stac/v1/", request_modifier=change_to_https)
    all_collections = csc_catalog.get_collections()
    csc_collection = next(collection for collection in all_collections if collection.id=="sentinel2-l2a")
    print(f"Updating STAC Catalog at {args.host}")
    update_catalog(app_host, csc_collection)

    end = time.time()
    print(f"Script took {round(end-start, 1)} seconds")