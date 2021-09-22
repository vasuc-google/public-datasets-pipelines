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


import json
import logging
import os
import pathlib
import typing
from datetime import datetime

import pandas as pd
import requests
from google.cloud import storage
from pandas.core.indexes.base import Index


def main(
    source_url: str,
    source_file: pathlib.Path,
    target_file: pathlib.Path,
    target_gcs_bucket: str,
    target_gcs_path: str,
    headers:typing.List[str],
    rename_mappings:dict,
):

    logging.info("creating 'files' folder")
    pathlib.Path("./files").mkdir(parents=True, exist_ok=True)

    logging.info(f"Downloading file {source_url}")
    download_file(source_url, source_file)
    df = pd.read_csv(str(source_file))

    logging.info(f"Transforming.. {source_file}")
    rename_headers(df, rename_mappings)
    date_convert(df)

    logging.info("Transform: Reordering headers..")
    df = df[headers]

    logging.info(f"Saving to output file.. {target_file}")
    try:
        save_to_new_file(df, file_path=str(target_file))
    except Exception as e:
        logging.error(f"Error saving output file: {e}.")
    logging.info("..Done!")


    logging.info(
        f"Uploading output file to.. gs://{target_gcs_bucket}/{target_gcs_path}"
     )
    upload_file_to_gcs(target_file, target_gcs_bucket, target_gcs_path)


def rename_headers(df, rename_mappings:dict):
    df=df.rename(columns=rename_mappings, inplace=True)

def convert_dt_format(dt_str):
    if dt_str is None or len(dt_str) == 0:
        return dt_str
    else:
        if len(dt_str) == 10:
            return datetime.strptime(dt_str, "%m/%d/%Y").strftime(
                "%Y-%m-%d"
            )
        else:
            return datetime.strptime(dt_str, "%m/%d/%Y %H:%M:%S %p").strftime(
                "%Y-%m-%d %H:%M:%S"
            )

def date_convert(df):
    dt_cols = [
        "report_date",
        "load_time"
    ]

    for dt_col in dt_cols:
        df[dt_col] = df[dt_col].apply(convert_dt_format)

def save_to_new_file(df, file_path):
    df.to_csv(file_path, index=False)


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
        headers=json.loads(os.environ["CSV_HEADERS"]),
        rename_mappings=json.loads(os.environ["RENAME_MAPPINGS"])
    )
