# Copyright 2021 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import pathlib
from datetime import datetime

import pandas as pd
import requests
from google.cloud import storage


def main(
    source_url: str,
    source_file: pathlib.Path,
    target_file: pathlib.Path,
    target_gcs_bucket: str,
    target_gcs_path: str,
):

    logging.info("creating 'files' folder")
    pathlib.Path("./files").mkdir(parents=True, exist_ok=True)
    
    logging.info(f"Downloading file {source_url}")
    download_file(source_url, source_file)

    # open the input file
    logging.info(f"Opening file {source_file}")
    # client = storage.Client()
    # # get the bucke
    # bucket = client.get_bucket("nttcomware")
    # # get the blob object
    # blob_name = "test.csv.gz"
    # blob = bucket.get_blob(blob_name)
    df = pd.read_csv(str(source_file), compression='gzip')
    
    # steps in the pipeline
    logging.info(f"Transforming.. {source_file}")
    rename_headers(df)
    logging.info("Transform: Reordering headers..")
    df = df[    
        [
            "granule_id",
            "product_id",
            "datatake_identifier",
            "mgrs_tile",
            "sensing_time",
            "geometric_quality_flag",
            "generation_time",
            "north_lat",
            "south_lat",
            "west_lon",
            "east_lon",
            "base_url",
            "total_size",
            "cloud_cover"
            
        ]
    ]

    # save to output file
    logging.info(f"Saving to output file.. {target_file}")
    try:
        save_to_new_file(df, file_path=str(target_file))
    except Exception as e:
        logging.error(f"Error saving output file: {e}.")
    logging.info("..Done!")

    # upload to GCS
    logging.info(
        f"Uploading output file to.. gs://{target_gcs_bucket}/{target_gcs_path}"
     )
    upload_file_to_gcs(target_file, target_gcs_bucket, target_gcs_path)


def rename_headers(df):
    header_names = {
        "GRANULE_ID" : "granule_id",
        "PRODUCT_ID" : "product_id",
        "DATATAKE_IDENTIFIER" : "datatake_identifier",
        "MGRS_TILE" : "mgrs_tile",
        "SENSING_TIME" : "sensing_time",
        "TOTAL_SIZE" : "total_size",
        "CLOUD_COVER" : "cloud_cover",
        "GEOMETRIC_QUALITY_FLAG" : "geometric_quality_flag",
        "GENERATION_TIME" : "generation_time",
        "NORTH_LAT" : "north_lat",
        "SOUTH_LAT" : "south_lat",
        "WEST_LON" : "west_lon",
        "EAST_LON" : "east_lon",
        "BASE_URL" : "base_url"
    }

    df=df.rename(columns=dict, inplace=True)

def save_to_new_file(df, file_path):
    df.export_csv(file_path)

def download_file(source_url: str, source_file: pathlib.Path):
    logging.info(f"Downloading {source_url} into {source_file}")
    r = requests.get(source_url, stream=True)
    if r.status_code == 200:
        with open(source_file, "wb") as f:
            for chunk in r:
                f.write(chunk)
    else:
        logging.error(f"Couldn't download {source_url}: {r.text}")


def upload_file_to_gcs(file_path: pathlib.Path, gcs_bucket: str, gcs_path: str) -> None:
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(file_path)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    main(
        source_url=os.environ["SOURCE_URL"],
        source_file=pathlib.Path(os.environ["SOURCE_FILE"]).expanduser(),
        target_file=pathlib.Path(os.environ["TARGET_FILE"]).expanduser(),
        target_gcs_bucket=os.environ["TARGET_GCS_BUCKET"],
        target_gcs_path=os.environ["TARGET_GCS_PATH"],
    )