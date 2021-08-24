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
    df = pd.read_csv(str(source_file))

    # steps in the pipeline
    logging.info(f"Transforming.. {source_file}")
    rename_headers(df)
    convert_dt_values(df)
    logging.info("Transform: Reordering headers..")
    df = df[    
        [
            "invoice_and_item_number",
            "date",
            "store_number",
            "store_name",
            "address",
            "city",
            "zip_code",
            "store_location",
            "county_number",
            "county",
            "category",
            "category_name",
            "vendor_number",
            "vendor_name",
            "item_number",
            "item_description",
            "pack",
            "bottle_volume_ml",
            "state_bottle_cost",
            "state_bottle_retail",
            "bottles_sold",
            "sale_dollars",
            "volume_sold_liters",
            "volume_sold_gallons"
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
        "Invoice/Item Number" : "invoice_and_item_number",
        "Date" : "date",
        "Store Number" : "store_number",
        "Store Name" : "store_name",
        "Address" : "address",
        "City" : "city",
        "Zip Code" : "zip_code",
        "Store Location" : "store_location",
        "County Number" : "county_number",
        "County" : "county",
        "Category" : "category",
        "Category Name" : "category_name",
        "Vendor Number" : "vendor_number",
        "Vendor Name" : "vendor_name",
        "Item Number" : "item_number",
        "Item Description" : "item_description",
        "Pack" : "pack",
        "Bottle Volume (ml)" : "bottle_volume_ml",
        "State Bottle Cost" : "state_bottle_cost",
        "State Bottle Retail" : "state_bottle_retail",
        "Bottles Sold" : "bottles_sold",
        "Sale (Dollars)" : "sale_dollars",
        "Volume Sold (Liters)" : "volume_sold_liters",
        "Volume Sold (Gallons)" : "volume_sold_gallons"
    }

    df=df.rename(columns=header_names, inplace=True)

def convert_dt_format(dt_str):
    if dt_str is None or len(dt_str) == 0:
        return dt_str
    else:
        if len(dt_str) == 10:
            return datetime.strptime(dt_str, "%m/%d/%Y").strftime(
                "%Y-%m-%d"
            )

def convert_dt_values(df):
    dt_cols = [
        "date"
    ]

    for dt_col in dt_cols:
        df[dt_col] = df[dt_col].apply(convert_dt_format)

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
