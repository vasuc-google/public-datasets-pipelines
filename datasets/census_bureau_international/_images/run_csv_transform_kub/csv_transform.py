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
import subprocess
import typing

import pandas as pd
from google.cloud import storage


def main(
    source_url: typing.List[str],
    source_file: typing.List[pathlib.Path],
    target_file: pathlib.Path,
    target_gcs_bucket: str,
    target_gcs_path: str,
    headers: typing.List[str],
    pipeline_name: str,
) -> None:
    logging.info("Creating 'files' folder")
    pathlib.Path("./files").mkdir(parents=True, exist_ok=True)

    logging.info(f"Downloading file {source_url}")
    download_file(source_url, source_file)

    logging.info(f"Reading the file {source_url}")
    df = read_files(source_file)

    if pipeline_name == "country_names_area":
        df = df
    else:
        logging.info("Filter headers ...")
        df = df.drop(["country_area"], axis=1)

    logging.info("Search and replacing the values..")
    if pipeline_name == "midyear_population_age_sex":
        df["sex"] = df["sex"].apply({2: "Male", 3: "Female"}.get)

    logging.info("Reordering headers..")
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


def save_to_new_file(df, file_path):
    df.to_csv(file_path, index=False)


def read_files(source_files: typing.List[str]) -> pd.DataFrame:
    df = pd.DataFrame()
    for source_file in source_files:
        _df = pd.read_csv(source_file)
        if df.empty:
            df = _df
        else:
            df = pd.merge(df, _df, how="left", on=["country_code"])
    return df


def download_file(
    source_url: typing.List[str], source_file: typing.List[pathlib.Path]
) -> None:
    for url, file in zip(source_url, source_file):
        logging.info(f"Downloading file from {url} ...")
        subprocess.check_call(["gsutil", "cp", f"{url}", f"{file}"])


def upload_file_to_gcs(file_path: pathlib.Path, gcs_bucket: str, gcs_path: str) -> None:
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(file_path)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    main(
        source_url=json.loads(os.environ["SOURCE_URL"]),
        source_file=json.loads(os.environ["SOURCE_FILE"]),
        target_file=pathlib.Path(os.environ["TARGET_FILE"]).expanduser(),
        target_gcs_bucket=os.environ["TARGET_GCS_BUCKET"],
        target_gcs_path=os.environ["TARGET_GCS_PATH"],
        headers=json.loads(os.environ["CSV_HEADERS"]),
        pipeline_name=os.environ["PIPELINE_NAME"],
    )
