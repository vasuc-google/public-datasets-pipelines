# Copyright 2022 Google LLC
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

import pandas as pd
import requests
from google.cloud import storage


def main(
    source_url: str,
    source_file: pathlib.Path,
    target_file: pathlib.Path,
    target_gcs_bucket: str,
    target_gcs_path: str,
    headers: typing.List[str],
    rename_mappings: dict
) -> None:
    logging.info("Creating 'files' folder")
    pathlib.Path("./files").mkdir(parents=True, exist_ok=True)
    download_file(source_url, source_file)
    df = pd.read_csv(str(source_file))
    df = create_geometry_columns(df)
    rename_headers(df, rename_mappings)
    df = reorder_headers(df, headers)
    save_to_new_file(df, file_path=str(target_file))
    upload_file_to_gcs(target_file, target_gcs_bucket, target_gcs_path)


def create_geometry_columns(df: pd.DataFrame) -> pd.DataFrame:
    logging.info(f"Transform: Creating Geometry Column")
    df["location_geom"] = (
        "POINT("
        + df["Long"].astype(str).replace("nan", "")
        + " "
        + df["Lat"].astype(str).replace("nan", "")
        + ")"
    )
    df.location_geom = df.location_geom.replace("POINT( )", "")
    return df


def rename_headers(df: pd.DataFrame,
                   rename_mappings: dict
    ) -> pd.DataFrame:
    logging.info("Transform: Renaming headers...")
    df = df.rename(columns=rename_mappings, inplace=True)


def reorder_headers(df: pd.DataFrame,
                    headers: typing.List[str]
    ) -> pd.DataFrame:
    logging.info("Transform: Reordering headers..")
    df = df[headers]
    return df


def save_to_new_file(df: pd.DataFrame,
                     file_path: str
    ) -> None:
    logging.info(f"Saving to output file.. {file_path}")
    try:
        df.to_csv(file_path, index=False)
    except Exception as e:
        logging.error(f"Error saving output file: {e}.")
    logging.info("..Done!")


def download_file(source_url: str,
                  source_file: pathlib.Path
    ) -> None:
    logging.info(f"Downloading {source_url} into {source_file}")
    r = requests.get(source_url, stream=True)
    if r.status_code == 200:
        with open(source_file, "wb") as f:
            for chunk in r:
                f.write(chunk)
    else:
        logging.error(f"Couldn't download {source_url}: {r.text}")


def upload_file_to_gcs(file_path: pathlib.Path,
                       target_gcs_bucket: str,
                       target_gcs_path: str
    ) -> None:
    if os.path.exists(file_path):
        logging.info(
            f"Uploading output file {file_path} to gs://{target_gcs_bucket}/{target_gcs_path}"
        )
        storage_client = storage.Client()
        bucket = storage_client.bucket(target_gcs_bucket)
        blob = bucket.blob(target_gcs_path)
        blob.upload_from_filename(str(file_path))
    else:
        logging.info(
            f"Cannot upload file to gs://{target_gcs_bucket}/{target_gcs_path} as it does not exist."
        )


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
