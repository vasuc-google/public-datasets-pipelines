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

import datetime
import logging
import os
import pathlib
from ftplib import FTP

import vaex
from google.cloud import storage


def main(
    source_url: str,
    ftp_host: str,
    ftp_dir: str,
    ftp_filename: str,
    source_file: pathlib.Path,
    target_file: pathlib.Path,
    target_gcs_bucket: str,
    target_gcs_path: str,
):

    # source_url            STRING          -> The full url of the source file to transform
    # ftp_host              STRING          -> The host IP of the ftp file (IP only)
    # ftp_dir               STRING          -> The remote working directory that the FTP file resides in (directory only)
    # ftp_filename          STRING          -> The name of the file to pull from the FTP site
    # source_file           PATHLIB.PATH    -> The (local) path pertaining to the downloaded source file
    # target_file           PATHLIB.PATH    -> The (local) target transformed file + filename
    # target_gcs_bucket     STRING          -> The target GCS bucket to place the output (transformed) file
    # target_gcs_path       STRING          -> The target GCS path ( within the GCS bucket ) to place the output (transformed) file

    logging.info(
        "GCHND Countries process started at "
        + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )

    logging.info(f"Downloading FTP file {source_url} from {ftp_host}")
    download_file_ftp(ftp_host, ftp_dir, ftp_filename, source_file, source_url)

    # insert header into downloaded file
    source_file_bak = str(source_file) + ".bak"
    logging.info(f"Adding column header to file {source_file}")
    os.system(
        'echo "code name" > '
        + str(source_file)
        + " && cat "
        + str(source_file_bak)
        + " >> "
        + str(source_file)
    )

    # open the input file
    logging.info(f"Opening file {source_file}")
    df = vaex.open(source_file, sep="|", convert=False)

    # steps in the pipeline
    logging.info(f"Transformation Process Starting.. {source_file}")

    # extract the country code from the single field value
    df["code"] = df["code name"].apply(get_column_country_code)

    # extract the country name from the single field value
    df["name"] = df["code name"].apply(get_column_country_name)

    # pdb.set_trace()
    # logging.info(f'Number of columns: {df.column_count(hidden=True)}')

    # reorder headers in output
    logging.info("Transform: Reordering headers..")
    df = df[
        [
            "code",
            "name",
        ]
    ]

    # steps in the pipeline
    logging.info(f"Transformation Process complete .. {source_file}")

    # save to output file
    logging.info(f"Saving to output file.. {target_file}")

    try:
        # save_to_new_file(df, file_path=str(target_file))
        save_to_new_file(df, file_path=str(target_file))
    except Exception as e:
        logging.error(f"Error saving output file: {e}.")

    logging.info(
        f"Uploading output file to.. gs://{target_gcs_bucket}/{target_gcs_path}"
    )
    upload_file_to_gcs(target_file, target_gcs_bucket, target_gcs_path)

    # log completion
    logging.info(
        "GCHND Countries process completed at "
        + str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))
    )


def get_column_country_code(col_val: str):
    # return the country code
    return col_val.strip().split(" ")[0]


def get_column_country_name(col_val: str):
    # find the length of country code
    len_code = len(str.split(str.strip(col_val), " ")[0])
    # grab and strip the entire field
    strmain1 = str.strip(col_val)
    # find the length of the entire field
    len_main = len(str.strip(col_val))
    # find the length of the entire field minus the length of the code
    # this will find the length of the country name
    len_out = len_main - len_code
    # return the text which is the entire column minus the country code
    # this is the actual country name
    return str.strip((strmain1[::-1])[0:(len_out)][::-1])


def convert_dt_format(date_str, time_str):
    #  date_str, time_str
    # 10/26/2014,13:12:00
    return str(datetime.datetime.strptime(date_str, "%m/%d/%Y").date()) + " " + time_str


def save_to_new_file(df, file_path):
    df.export_csv(file_path, float_format="%.0f")
    # df.to_csv(file_path, float_format="%.0f")


def download_file_ftp(
    ftp_host: str,
    ftp_dir: str,
    ftp_filename: str,
    local_file: pathlib.Path,
    source_url: str,
):

    # ftp_host      -> host ip (eg. 123.123.0.1)
    # ftp_dir       -> working directory in ftp host where file is located
    # ftp_filename  -> filename of FTP file to download
    # source_file   -> local file (including path) to create containing ftp content

    logging.info(f"Downloading {source_url} into {local_file}")
    ftp_conn = FTP(ftp_host)
    ftp_conn.login("", "")
    ftp_conn.cwd(ftp_dir)
    # ftp_conn.encoding = 'utf-8'

    try:
        bak_local_file = str(local_file) + ".bak"
        dest_file = open(bak_local_file, "wb")
        ftp_conn.encoding = "utf-8"
        ftp_conn.retrbinary(
            cmd="RETR " + ftp_filename,
            callback=dest_file.write,
            blocksize=1024,
            rest=None,
        )
        ftp_conn.quit()
        dest_file.close()
    except Exception as e:
        logging.error(f"Error saving output file: {e}.")


def upload_file_to_gcs(file_path: pathlib.Path, gcs_bucket: str, gcs_path: str) -> None:
    storage_client = storage.Client()
    bucket = storage_client.bucket(gcs_bucket)
    blob = bucket.blob(gcs_path)
    blob.upload_from_filename(file_path)


if __name__ == "__main__":
    logging.getLogger().setLevel(logging.INFO)

    main(
        source_url=os.environ["SOURCE_URL"],
        ftp_host=os.environ["FTP_HOST"],
        ftp_dir=os.environ["FTP_DIR"],
        ftp_filename=os.environ["FTP_FILENAME"],
        source_file=pathlib.Path(os.environ["SOURCE_FILE"]).expanduser(),
        target_file=pathlib.Path(os.environ["TARGET_FILE"]).expanduser(),
        target_gcs_bucket=os.environ["TARGET_GCS_BUCKET"],
        target_gcs_path=os.environ["TARGET_GCS_PATH"],
    )
