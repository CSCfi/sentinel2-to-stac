import boto3
import pystac as stac
import rasterio
import logging
import re
import os
import pandas as pd
from itertools import chain
from datetime import datetime
from xml.dom import minidom
from shapely.geometry import box, mapping, GeometryCollection, shape
from pystac.extensions.eo import EOExtension, Band
from pystac.extensions.projection import ProjectionExtension
from pystac import (Catalog, CatalogType)

from rasterio.warp import transform_bounds
from rasterio.crs import CRS

from botocore import UNSIGNED
from botocore.client import Config

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

def init_client():

    # Create client with credentials. Allas-conf needed to be run for boto3 to get the credentials
    s3 = boto3.client(
        service_name = "s3",
        endpoint_url = "https://a3s.fi", 
        region_name = "regionOne"
    )

    return s3

def get_buckets(client):
    """
        client: boto3.client
    """
    # Get the bucket names from the README-file
    # readme = client.get_object(Bucket='sentinel-readme', Key='uploadedByMariaYliHeikkila.txt')
    # buckets_readme = readme['Body'].read().splitlines()
    
    # Separate the bucket names and provide one bucket only once
    # buckets = list(set(list(map(lambda x: x.decode().split('//',1)[1].split('/',1)[0], buckets_readme))))

    bucket_information = client.list_buckets()
    buckets = [x['Name'] for x in bucket_information['Buckets'] if re.match(r"Sentinel2(?!.*segments)", x['Name'])]

    first_csv = pd.read_table("2000290_buckets.csv", header=None)
    first_buckets = list(chain.from_iterable(first_csv.to_numpy()))
    second_csv = pd.read_table("2001106_buckets.csv", header=None)
    second_buckets = list(chain.from_iterable(second_csv.to_numpy()))
    
    buckets = [*buckets, *first_buckets, *second_buckets]

    return buckets

def create_collection(client, buckets):
    """
        client: boto3.client
        buckets: list of bucket names where data will be found
    """

    rootcollection = make_root_collection()
    rootcatalog = stac.Catalog(id='Sentinel-2 catalog', description='Sentinel 2 catalog.')
    rootcatalog.add_child(rootcollection)
    
    for bucket in buckets:

        # Usual list_objects_v2 function only lists up to 1000 objects so pagination is needed when using a client
        paginator = client.get_paginator('list_objects_v2')
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
        print('Bucket:', bucket)
        print('SAFES:', listofsafes)

        for i, safe in enumerate(listofsafes):
            #print('SAFE index:', i)
            #print('SAFE name:', safe)

            # SAFE-filename without the subfix
            safename = str(safe.split('.')[0])

            metadatafile = ''.join((x for x in bucketcontent_mtd if safename in x))
            crsmetadatafile = ''.join((x for x in bucketcontent_crs if safename in x))
            if not metadatafile or not crsmetadatafile:
                # If there is no metadatafile or CRS-metadatafile, the SAFE does not include data relevant to the script
                continue
            # THIS FAILS WITH FOLDER BUCKETS
            logging.log(logging.INFO, f"CRS: {crsmetadatafile}")
            safecrs_metadata = get_crs(get_metadata_content(bucket, crsmetadatafile, client))
            
            # only jp2 that are image bands
            jp2images = [x for x in bucketcontent_jp2 if safe in x and 'IMG_DATA' in x]
            # if there are no jp2 imagefiles in the bucket, continue to the next bucket
            if not jp2images:
                continue
            # jp2 that are preview images
            previewimage = next(x for x in bucketcontent_jp2 if safe in x and 'PVI' in x)

            metadatacontent = get_metadata_content(bucket, metadatafile, client)
            
            for i, jp2image in enumerate(jp2images):

                #print('Image index:', i)

                uri = 'https://a3s.fi/' + bucket + '/' + jp2image
                #print('Image URI:', uri)

                items = list(rootcollection.get_items())
                # Check if the item in question is already added to the collection
                if safename not in [x.id for x in items]:
                    item = make_item(uri, metadatacontent, safecrs_metadata)
                    rootcollection.add_item(item)
                    # add preview image 
                    add_asset(item, 'https://a3s.fi/' + bucket + '/' + previewimage, None, True)

                else:
                    item = [x for x in items if safename in x.id][0]
                    add_asset(item, uri, safecrs_metadata)
        
        # Save and normalize after each bucket
        rootcatalog.normalize_and_save('Sentinel2-tileless', catalog_type=CatalogType.RELATIVE_PUBLISHED, skip_unresolved=True)

    rootcatalog.normalize_hrefs('Sentinel2-tileless')
    rootcatalog.validate_all()

    # Update the spatial and temporal extent
    print('Updating collection extent')
    rootbounds = [GeometryCollection([shape(s.geometry) for s in rootcollection.get_all_items()]).bounds]
    roottimes = [t.datetime for t in rootcollection.get_all_items()]
    roottemporal = [[min(roottimes), max(roottimes)]]
    rootcollection.extent.spatial = stac.SpatialExtent(rootbounds)
    rootcollection.extent.temporal = stac.TemporalExtent(roottemporal)

    rootcatalog.save(catalog_type=CatalogType.RELATIVE_PUBLISHED)

    print('Catalog saved')

def make_root_collection():

    # Preliminary apprx Finland, later with bbox of all tiles from bucketname
    sp_extent = stac.SpatialExtent([[0,0,0,0]])
    # Fill with general Sentinel-2 timeframe, later get from all safefiles
    capture_date = datetime.strptime('2015-06-29', '%Y-%m-%d')
    tmp_extent = stac.TemporalExtent([(capture_date, datetime.today())])
    extent = stac.Extent(sp_extent, tmp_extent)

    # Added optional stac_extension
    rootcollection = stac.Collection(
        id = 'sentinel2-l2a',
        title = 'Sentinel-2 L2A',
        description = 'Sentinel-2 products, processed to Level-2A (Surface Reflectance), a selection of mostly cloud-free products from Finland. More information: https://a3s.fi/sentinel-readme/README.txt',
        extent = extent, 
        stac_extensions = [
            "https://stac-extensions.github.io/eo/v1.0.0/schema.json",
            "https://stac-extensions.github.io/projection/v1.0.0/schema.json"
        ],
        license = 'CC-BY-3.0-IGO',
        providers = [stac.Provider(
            name = "CSC Finland",
            url = "https://www.csc.fi/",
            roles = ["host"]
        )],
        summaries = stac.Summaries(
            summaries={
                "eo:bands": [value for k, value in s2_bands_as_dict.items()],
                "gsd": [10, 20, 60]
            }
        )
    )
    # Add the link to the license
    rootcollection.add_link(
        link=stac.Link(
            rel = 'licence',
            target = 'https://sentinel.esa.int/documents/247904/690755/Sentinel_Data_Legal_Notice'
        )
    )
    # Add README as metadata assett
    rootcollection.add_asset(
        key="metadata",
        asset=stac.Asset(
            roles=['metadata'],
            href='https://a3s.fi/sentinel-readme/README.txt'
        )
    )

    print('Root collection made')

    return rootcollection

def make_item(uri, metadatacontent, crs_metadata):
    """
        uri: The SAFE ID of the item (currently URL of the image, could be changes to SAFE later)
        metadatacontent: Metadata dict got from get_metadata_content()
        crs_metadata: CRS metadata dict containing CRS string and shapes for different resolutions from get_crs()
    """

    logging.info(uri)
    #print('Making item')
    params = {}

    if re.match(r".+?\d{4}/S2(A|B)", uri):
        params['id'] = uri.split("/")[5].split('.')[0]
    else:
        params['id'] = uri.split('/')[4].split('.')[0]

    #params['id'] = uri.split('/')[4].split('.')[0]
    
    with rasterio.open(uri) as src:

        #print(list([src.bounds]))
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

    stacItem = stac.Item(**params)

    # Adding the EO and Projecting Extensions to the item
    eo_ext = EOExtension.ext(stacItem, add_if_missing=True)
    eo_ext.bands = [s2_bands[band]['band'] for band in s2_bands]
    proj_ext = ProjectionExtension.ext(stacItem, add_if_missing=True)
    proj_ext.apply(epsg = int(crs_metadata['CRS']), transform = item_transform)

    print('Item made:', params['id'])

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
        asset = stac.Asset(
            href=uri,
            title=full_bandname,
            media_type=stac.MediaType.JPEG2000,
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
        asset = stac.Asset(
                href=uri,
                title=full_bandname,
                media_type=stac.MediaType.JPEG2000,
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
        asset = stac.Asset(
                href=uri,
                title="Thumbnail image",
                media_type=stac.MediaType.JPEG2000,
                roles=["thumbnail"],
                extra_fields= {
                    'proj:shape': shape,
                }
        )
        stacItem.add_asset(
            key="thumbnail", 
            asset=asset
        )

    #print(f'Asset added: {full_bandname}')

    return stacItem

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
        #metadatadict['bbox'] = get_xml_content(doc,'EXT_POS_LIST')
        metadatadict['orbit'] = get_xml_content(doc,'SENSING_ORBIT_NUMBER')
        metadatadict['baseline'] = get_xml_content(doc,'PROCESSING_BASELINE')
        #metadatadict['producttype'] = get_xml_content(doc,'PRODUCT_TYPE')
        #metadatadict['productname'] = get_xml_content(doc,'PRODUCT_URI').split('.')[0]

    #print('Metadata extracted')

    return metadatadict

if __name__ == '__main__':

    logging.basicConfig(filename='debug.log', level=logging.INFO)

    s3 = init_client()
    buckets = get_buckets(s3)
    create_collection(s3, buckets)