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


from airflow import DAG
from airflow.providers.cncf.kubernetes.operators import kubernetes_pod
from airflow.providers.google.cloud.transfers import gcs_to_bigquery

default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "start_date": "2021-03-01",
}


with DAG(
    dag_id="epa_historical_air_quality.voc_daily_summary",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="30 13 * * *",
    catchup=False,
    default_view="graph",
) as dag:

    # Run CSV transform within kubernetes pod
    transform_csv = kubernetes_pod.KubernetesPodOperator(
        task_id="transform_csv",
        name="voc_daily_summary",
        namespace="composer",
        service_account_name="datasets",
        image_pull_policy="Always",
        image="{{ var.json.epa_historical_air_quality.container_registry.run_csv_transform_kub }}",
        env_vars={
            "SOURCE_URL": "https://aqs.epa.gov/aqsweb/airdata/daily_VOCS_YEAR_ITERATOR.zip",
            "START_YEAR": "1990",
            "SOURCE_FILE": "files/data.csv",
            "TARGET_FILE": "files/data_output.csv",
            "CHUNKSIZE": "2500000",
            "TARGET_GCS_BUCKET": "{{ var.value.composer_bucket }}",
            "TARGET_GCS_PATH": "data/epa_historical_air_quality/voc_daily_summary/files/data_output.csv",
            "DATA_NAMES": '[ "state_code", "county_code", "site_num", "parameter_code", "poc",\n  "latitude", "longitude", "datum", "parameter_name", "sample_duration",\n  "pollutant_standard", "date_local", "units_of_measure", "event_type", "observation_count",\n  "observation_percent", "arithmetic_mean", "first_max_value", "first_max_hour", "aqi",\n  "method_code", "method_name", "local_site_name", "address", "state_name",\n  "county_name", "city_name", "cbsa_name", "date_of_last_change" ]',
            "DATA_DTYPES": '{ "state_code": "str", "county_code": "str", "site_num": "str", "parameter_code": "int32", "poc": "int32",\n  "latitude": "float64", "longitude": "float64", "datum": "str", "parameter_name": "str", "sample_duration": "str",\n  "pollutant_standard": "str", "date_local": "datetime64[ns]", "units_of_measure": "str", "event_type": "str", "observation_count": "int32",\n  "observation_percent": "float64", "arithmetic_mean": "float64", "first_max_value": "float64", "first_max_hour": "int32", "aqi": "str",\n  "method_code": "str", "method_name": "str", "local_site_name": "str", "address": "str", "state_name": "str",\n  "county_name": "str", "city_name": "str", "cbsa_name": "str", "date_of_last_change": "datetime64[ns]" }',
        },
        resources={
            "request_memory": "8G",
            "request_cpu": "3",
            "request_ephemeral_storage": "5G",
        },
    )

    # Task to load CSV data to a BigQuery table
    load_to_bq = gcs_to_bigquery.GCSToBigQueryOperator(
        task_id="load_to_bq",
        bucket="{{ var.value.composer_bucket }}",
        source_objects=[
            "data/epa_historical_air_quality/voc_daily_summary/files/data_output.csv"
        ],
        source_format="CSV",
        destination_project_dataset_table="{{ var.json.epa_historical_air_quality.destination_tables.voc_daily_summary }}",
        skip_leading_rows=1,
        allow_quoted_newlines=True,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {
                "name": "state_code",
                "type": "STRING",
                "description": "The FIPS code of the state in which the monitor resides.",
                "mode": "NULLABLE",
            },
            {
                "name": "county_code",
                "type": "STRING",
                "description": "The FIPS code of the county in which the monitor resides.",
                "mode": "NULLABLE",
            },
            {
                "name": "site_num",
                "type": "STRING",
                "description": "A unique number within the county identifying the site.",
                "mode": "NULLABLE",
            },
            {
                "name": "parameter_code",
                "type": "INTEGER",
                "description": "The AQS code corresponding to the parameter measured by the monitor.",
                "mode": "NULLABLE",
            },
            {
                "name": "poc",
                "type": "INTEGER",
                "description": "This is the “Parameter Occurrence Code” used to distinguish different instruments that measure the same parameter at the same site.",
                "mode": "NULLABLE",
            },
            {
                "name": "latitude",
                "type": "FLOAT",
                "description": "The monitoring site’s angular distance north of the equator measured in decimal degrees.",
                "mode": "NULLABLE",
            },
            {
                "name": "longitude",
                "type": "FLOAT",
                "description": "The monitoring site’s angular distance east of the prime meridian measured in decimal degrees.",
                "mode": "NULLABLE",
            },
            {
                "name": "datum",
                "type": "STRING",
                "description": "The Datum associated with the Latitude and Longitude measures.",
                "mode": "NULLABLE",
            },
            {
                "name": "parameter_name",
                "type": "STRING",
                "description": "The name or description assigned in AQS to the parameter measured by the monitor. Parameters may be pollutants or non-pollutants.",
                "mode": "NULLABLE",
            },
            {
                "name": "sample_duration",
                "type": "STRING",
                "description": "The length of time that air passes through the monitoring device before it is analyzed (measured). So, it represents an averaging period in the atmosphere (for example, a 24-hour sample duration draws ambient air over a collection filter for 24 straight hours). For continuous monitors, it can represent an averaging time of many samples (for example, a 1-hour value may be the average of four one-minute samples collected during each quarter of the hour).",
                "mode": "NULLABLE",
            },
            {
                "name": "pollutant_standard",
                "type": "STRING",
                "description": "A description of the ambient air quality standard rules used to aggregate statistics. (See description at beginning of document.)",
                "mode": "NULLABLE",
            },
            {
                "name": "date_local",
                "type": "TIMESTAMP",
                "description": "The calendar date for the summary. All daily summaries are for the local standard day (midnight to midnight) at the monitor.",
                "mode": "NULLABLE",
            },
            {
                "name": "units_of_measure",
                "type": "STRING",
                "description": "The unit of measure for the parameter. QAD always returns data in the standard units for the parameter. Submitters are allowed to report data in any unit and EPA converts to a standard unit so that we may use the data in calculations.",
                "mode": "NULLABLE",
            },
            {
                "name": "event_type",
                "type": "STRING",
                "description": "Indicates whether data measured during exceptional events are included in the summary. A wildfire is an example of an exceptional event; it is something that affects air quality, but the local agency has no control over. No Events means no events occurred. Events Included means events occurred and the data from them is included in the summary. Events Excluded means that events occurred but data form them is excluded from the summary. Concurred Events Excluded means that events occurred but only EPA concurred exclusions are removed from the summary. If an event occurred for the parameter in question, the data will have multiple records for each monitor.",
                "mode": "NULLABLE",
            },
            {
                "name": "observation_count",
                "type": "INTEGER",
                "description": "The number of observations (samples) taken during the day.",
                "mode": "NULLABLE",
            },
            {
                "name": "observation_percent",
                "type": "FLOAT",
                "description": "The percent representing the number of observations taken with respect to the number scheduled to be taken during the day. This is only calculated for monitors where measurements are required (e.g., only certain parameters).",
                "mode": "NULLABLE",
            },
            {
                "name": "arithmetic_mean",
                "type": "FLOAT",
                "description": "The average (arithmetic mean) value for the day.",
                "mode": "NULLABLE",
            },
            {
                "name": "first_max_value",
                "type": "FLOAT",
                "description": "The highest value for the day.",
                "mode": "NULLABLE",
            },
            {
                "name": "first_max_hour",
                "type": "INTEGER",
                "description": "The hour (on a 24-hour clock) when the highest value for the day (the previous field) was taken.",
                "mode": "NULLABLE",
            },
            {
                "name": "aqi",
                "type": "INTEGER",
                "description": "The Air Quality Index for the day for the pollutant, if applicable.",
                "mode": "NULLABLE",
            },
            {
                "name": "method_code",
                "type": "INTEGER",
                "description": "An internal system code indicating the method (processes, equipment, and protocols) used in gathering and measuring the sample. The method name is in the next column.",
                "mode": "NULLABLE",
            },
            {
                "name": "method_name",
                "type": "STRING",
                "description": "A short description of the processes, equipment, and protocols used in gathering and measuring the sample.",
                "mode": "NULLABLE",
            },
            {
                "name": "local_site_name",
                "type": "STRING",
                "description": "The name of the site (if any) given by the State, local, or tribal air pollution control agency that operates it.",
                "mode": "NULLABLE",
            },
            {
                "name": "address",
                "type": "STRING",
                "description": "The approximate street address of the monitoring site.",
                "mode": "NULLABLE",
            },
            {
                "name": "state_name",
                "type": "STRING",
                "description": "The name of the state where the monitoring site is located.",
                "mode": "NULLABLE",
            },
            {
                "name": "county_name",
                "type": "STRING",
                "description": "The name of the county where the monitoring site is located.",
                "mode": "NULLABLE",
            },
            {
                "name": "city_name",
                "type": "STRING",
                "description": "The name of the city where the monitoring site is located. This represents the legal incorporated boundaries of cities and not urban areas.",
                "mode": "NULLABLE",
            },
            {
                "name": "cbsa_name",
                "type": "STRING",
                "description": "The name of the core bases statistical area (metropolitan area) where the monitoring site is located.",
                "mode": "NULLABLE",
            },
            {
                "name": "date_of_last_change",
                "type": "TIMESTAMP",
                "description": "The date the last time any numeric values in this record were updated in the AQS data system.",
                "mode": "NULLABLE",
            },
        ],
    )

    transform_csv >> load_to_bq
