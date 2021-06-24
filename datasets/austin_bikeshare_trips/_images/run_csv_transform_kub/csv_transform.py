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

# CSV transform for: austin_311.311_service_request


import logging
import os
import pathlib
import re
from datetime import datetime

import requests

# import vaex
from google.cloud import storage


def main(
    source_url: str,
    source_file: pathlib.Path,
    target_file: pathlib.Path,
    target_gcs_bucket: str,
    target_gcs_path: str,
):
    logging.info(f"Downloading file {source_url}")
    download_file(source_url, source_file)

    # # open the input file
    # logging.info(f"Opening file {source_file}")
    # df = vaex.open(str(source_file))

    # # steps in the pipeline
    # logging.info(f"Transforming.. {source_file}")
    # rename_headers(df)
    # remove_columns(df)
    # convert_dt_values(df)
    # replace_values_regex(df)
    # filter_null_rows(df)

    # # save to output file
    # logging.info(f"Saving to output file.. {target_file}")
    # try:
    #     save_to_new_file(df, file_path=str(target_file))
    # except Exception as e:
    #     logging.error(f"Error saving output file: {e}.")
    logging.info("..Done!")

    # upload to GCS
    logging.info(
        f"Uploading output file to.. gs://{target_gcs_bucket}/{target_gcs_path}"
    )
    upload_file_to_gcs(target_file, target_gcs_bucket, target_gcs_path)


def rename_headers(df):
    header_names = {
        "Trip ID": "trip_id",
        "Membership Type": "subscriber_type",
        "Bicycle ID": "bikeid",
        "Checkout Date": "start_time",
        "Checkout Kiosk ID": "start_station_id",
        "Checkout Kiosk": "start_station_name",
        "Return Kiosk ID": "end_station_id",
        "Return Kiosk": "end_station_name",
        "Trip Duration Minutes": "duration_minutes",
        "Checkout Time": "checkout_time",
        "Month": "month",
        "Year": "year",
    }

    for old_name, new_name in header_names.items():
        df.rename(old_name, new_name)


def remove_columns(df):
    header_names = {
        "start_time",
        "checkout_time",
        "month",
        "year",
    }

    for rm_name in header_names:
        df = df.drop(rm_name)


def convert_dt_format(dt_str):
    # Old format: MM/dd/yyyy hh:mm:ss aa
    # New format: yyyy-MM-dd HH:mm:ss
    if dt_str is None or len(dt_str) == 0:
        return dt_str
    else:
        if len(dt_str) == 10:
            return datetime.strptime(dt_str, "%m/%d/%Y").strftime("%Y-%m-%d %H:%M:%S")
        else:
            return datetime.strptime(dt_str, "%m/%d/%Y %H:%M:%S %p").strftime(
                "%Y-%m-%d %H:%M:%S"
            )


def convert_dt_values(df):
    dt_cols = ["start_time"]

    for dt_col in dt_cols:
        df[dt_col] = df[dt_col].apply(convert_dt_format)


def replace_value(val):
    if val is None or len(val) == 0:
        return val
    else:
        if val.find("\n") > 0:
            # return val.replace("\n", "")
            return re.sub(r"(^\d):(\d{2}:\d{2})", "0$1:$2", val)
        else:
            return val


def replace_values_regex(df):
    header_names = {"checkout_time"}

    for dt_col in header_names:
        if df[dt_col] is not None:
            if df[dt_col].str.len() > 0:
                df[dt_col] = df[dt_col].apply(replace_value)


def filter_null_rows(df):
    df = df[df.trip_id != ""]


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
