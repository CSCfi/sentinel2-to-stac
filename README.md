# Sentinel 2 to STAC

Python scripts to create a STAC collection from specific Sentinel 2 data and upload them to GeoServer. For getting the buckets through the boto3, you need read access to the CSC Project they are located in. Using the scripts on Linux, you need the allas_conf script for accessing Allas. The two CSV-files contain the buckets from these two CSC projects.

To run sentinel_to_stac.py:
```
$ python sentinel_to_stac.py
```
To run stac_to_geoserver.py, you need the GeoServer password which the code asks for at the beginning of the script:
```
$ python stac_to_geoserver.py
Password:
```
The post_stac.py is a testing script which was used to upload data to STAC FastAPI.