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
    dag_id="epa_historical_air_quality.annual_summaries",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="0 0 * * *",
    catchup=False,
    default_view="graph",
) as dag:

    # Run CSV transform within kubernetes pod
    transform_csv = kubernetes_pod.KubernetesPodOperator(
        task_id="transform_csv",
        name="annual_summaries",
        namespace="composer",
        service_account_name="datasets",
        image_pull_policy="Always",
        image="{{ var.json.epa_historical_air_quality.container_registry.run_csv_transform_kub }}",
        env_vars={
            "SOURCE_URL": "https://aqs.epa.gov/aqsweb/airdata/annual_conc_by_monitor_YEAR_ITERATOR.zip",
            "START_YEAR": "1980",
            "SOURCE_FILE": "files/data.csv",
            "TARGET_FILE": "files/data_output.csv",
            "CHUNKSIZE": "750000",
            "TARGET_GCS_BUCKET": "{{ var.value.composer_bucket }}",
            "TARGET_GCS_PATH": "data/epa_historical_air_quality/annual_summaries/files/data_output.csv",
            "DATA_NAMES": '[ "state_code", "county_code", "site_num", "parameter_code", "poc",\n  "latitude", "longitude", "datum", "parameter_name", "sample_duration",\n  "pollutant_standard", "metric_used", "method_name", "year", "units_of_measure",\n  "event_type", "observation_count", "observation_percent", "completeness_indicator", "valid_day_count",\n  "required_day_count", "exceptional_data_count", "null_data_count", "primary_exceedance_count", "secondary_exceedance_count",\n  "certification_indicator", "num_obs_below_mdl", "arithmetic_mean", "arithmetic_standard_dev", "first_max_value",\n  "first_max_datetime", "second_max_value", "second_max_datetime", "third_max_value", "third_max_datetime",\n  "fourth_max_value", "fourth_max_datetime", "first_max_non_overlapping_value", "first_no_max_datetime", "second_max_non_overlapping_value",\n  "second_no_max_datetime", "ninety_nine_percentile", "ninety_eight_percentile", "ninety_five_percentile", "ninety_percentile",\n  "seventy_five_percentile", "fifty_percentile", "ten_percentile", "local_site_name", "address",\n  "state_name", "county_name", "city_name", "cbsa_name", "date_of_last_change"]',
            "DATA_DTYPES": '{ "state_code": "str", "county_code": "str", "site_num": "str", "parameter_code": "int32", "poc": "int32",\n  "latitude": "float64", "longitude": "float64", "datum": "str", "parameter_name": "str", "sample_duration": "str",\n  "pollutant_standard": "str", "metric_used": "str", "method_name": "str", "year": "int32", "units_of_measure": "str",\n  "event_type": "str", "observation_count": "int32", "observation_percent": "float64", "completeness_indicator": "str", "valid_day_count": "int32",\n  "required_day_count": "int32", "exceptional_data_count": "int32", "null_data_count": "int32", "primary_exceedance_count": "str", "secondary_exceedance_count": "str",\n  "certification_indicator": "str", "num_obs_below_mdl": "int32", "arithmetic_mean": "float64", "arithmetic_standard_dev": "float64", "first_max_value": "float64",\n  "first_max_datetime": "datetime64[ns]", "second_max_value": "float64", "second_max_datetime": "datetime64[ns]", "third_max_value": "float64", "third_max_datetime": "datetime64[ns]",\n  "fourth_max_value": "float64", "fourth_max_datetime": "datetime64[ns]", "first_max_non_overlapping_value": "float64", "first_no_max_datetime": "datetime64[ns]", "second_max_non_overlapping_value": "float64",\n  "second_no_max_datetime": "datetime64[ns]", "ninety_nine_percentile": "float64", "ninety_eight_percentile": "float64", "ninety_five_percentile": "float64", "ninety_percentile": "float64",\n  "seventy_five_percentile": "float64", "fifty_percentile": "float64", "ten_percentile": "float64", "local_site_name": "str", "address": "str",\n  "state_name": "str", "county_name": "str", "city_name": "str", "cbsa_name": "str", "date_of_last_change": "datetime64[ns]" }',
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
            "data/epa_historical_air_quality/annual_summaries/files/data_output.csv"
        ],
        source_format="CSV",
        destination_project_dataset_table="{{ var.json.epa_historical_air_quality.destination_tables.annual_summaries }}",
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
                "description": "This is the 'Parameter Occurrence Code' used to distinguish different instruments that measure the same parameter at the same site.",
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
                "name": "metric_used",
                "type": "STRING",
                "description": "The base metric used in the calculation of the aggregate statistics presented in the remainder of the row. For example, if this is Daily Maximum, then the value in the Mean column is the mean of the daily maximums.",
                "mode": "NULLABLE",
            },
            {
                "name": "method_name",
                "type": "STRING",
                "description": "A short description of the processes, equipment, and protocols used in gathering and measuring the sample.",
                "mode": "NULLABLE",
            },
            {
                "name": "year",
                "type": "INTEGER",
                "description": "The year the annual summary data represents.",
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
                "description": "The number of observations (samples) taken during the year.",
                "mode": "NULLABLE",
            },
            {
                "name": "observation_percent",
                "type": "FLOAT",
                "description": "The percent representing the number of observations taken with respect to the number scheduled to be taken during the year. This is only calculated for monitors where measurements are required (e.g., only certain parameters).",
                "mode": "NULLABLE",
            },
            {
                "name": "completeness_indicator",
                "type": "STRING",
                "description": "An indication of whether the regulatory data completeness criteria for valid summary data have been met by the monitor for the year. Y means yes, N means no or that there are no regulatory completeness criteria for the parameter.",
                "mode": "NULLABLE",
            },
            {
                "name": "valid_day_count",
                "type": "INTEGER",
                "description": "The number of days during the year where the daily monitoring criteria were met, if the calculation of the summaries is based on valid days.",
                "mode": "NULLABLE",
            },
            {
                "name": "required_day_count",
                "type": "INTEGER",
                "description": "The number of days during the year which the monitor was scheduled to take samples if measurements are required.",
                "mode": "NULLABLE",
            },
            {
                "name": "exceptional_data_count",
                "type": "INTEGER",
                "description": "The number of data points in the annual data set affected by exceptional air quality events (things outside the norm that affect air quality).",
                "mode": "NULLABLE",
            },
            {
                "name": "null_data_count",
                "type": "INTEGER",
                "description": "The count of scheduled samples when no data was collected and the reason for no data was reported.",
                "mode": "NULLABLE",
            },
            {
                "name": "primary_exceedance_count",
                "type": "INTEGER",
                "description": "The number of samples during the year that exceeded the primary air quality standard.",
                "mode": "NULLABLE",
            },
            {
                "name": "secondary_exceedance_count",
                "type": "INTEGER",
                "description": "The number of samples during the year that exceeded the secondary air quality standard.",
                "mode": "NULLABLE",
            },
            {
                "name": "certification_indicator",
                "type": "STRING",
                "description": "An indication whether the completeness and accuracy of the information on the annual summary record has been certified by the submitter. Certified means the submitter has certified the data (due May 01 the year after collection). Certification not required means that the parameter does not require certification or the deadline has not yet passed. Uncertified (past due) means that certification is required but is overdue. Requested but not yet concurred means the submitter has completed the process, but EPA has not yet acted to certify the data. Requested but denied means the submitter has completed the process, but EPA has denied the request for cause. Was certified but data changed means the data was certified but data was replaced and the process has not been repeated.",
                "mode": "NULLABLE",
            },
            {
                "name": "num_obs_below_mdl",
                "type": "INTEGER",
                "description": "The number of samples reported during the year that were below the method detection limit (MDL) for the monitoring instrument. Sometimes these values are replaced by 1/2 the MDL in summary calculations.",
                "mode": "NULLABLE",
            },
            {
                "name": "arithmetic_mean",
                "type": "FLOAT",
                "description": "The average (arithmetic mean) value for the year.",
                "mode": "NULLABLE",
            },
            {
                "name": "arithmetic_standard_dev",
                "type": "FLOAT",
                "description": "The standard deviation about the mean of the values for the year.",
                "mode": "NULLABLE",
            },
            {
                "name": "first_max_value",
                "type": "FLOAT",
                "description": "The highest value for the year.",
                "mode": "NULLABLE",
            },
            {
                "name": "first_max_datetime",
                "type": "TIMESTAMP",
                "description": "The date and time (on a 24-hour clock) when the highest value for the year (the previous field) was taken.",
                "mode": "NULLABLE",
            },
            {
                "name": "second_max_value",
                "type": "FLOAT",
                "description": "The second highest value for the year.",
                "mode": "NULLABLE",
            },
            {
                "name": "second_max_datetime",
                "type": "TIMESTAMP",
                "description": "The date and time (on a 24-hour clock) when the second highest value for the year (the previous field) was taken.",
                "mode": "NULLABLE",
            },
            {
                "name": "third_max_value",
                "type": "FLOAT",
                "description": "The third highest value for the year.",
                "mode": "NULLABLE",
            },
            {
                "name": "third_max_datetime",
                "type": "TIMESTAMP",
                "description": "The date and time (on a 24-hour clock) when the third highest value for the year (the previous field) was taken.",
                "mode": "NULLABLE",
            },
            {
                "name": "fourth_max_value",
                "type": "FLOAT",
                "description": "The fourth highest value for the year.",
                "mode": "NULLABLE",
            },
            {
                "name": "fourth_max_datetime",
                "type": "TIMESTAMP",
                "description": "The date and time (on a 24-hour clock) when the fourth highest value for the year (the previous field) was taken.",
                "mode": "NULLABLE",
            },
            {
                "name": "first_max_non_overlapping_value",
                "type": "FLOAT",
                "description": "For 8-hour CO averages, the highest value of the year.",
                "mode": "NULLABLE",
            },
            {
                "name": "first_no_max_datetime",
                "type": "TIMESTAMP",
                "description": "The date and time (on a 24-hour clock) when the first maximum non overlapping value for the year (the previous field) was taken.",
                "mode": "NULLABLE",
            },
            {
                "name": "second_max_non_overlapping_value",
                "type": "FLOAT",
                "description": "For 8-hour CO averages, the second highest value of the year that does not share any hours with the 8-hour period of the first max non overlapping value.",
                "mode": "NULLABLE",
            },
            {
                "name": "second_no_max_datetime",
                "type": "TIMESTAMP",
                "description": "The date and time (on a 24-hour clock) when the second maximum non overlapping value for the year (the previous field) was taken.",
                "mode": "NULLABLE",
            },
            {
                "name": "ninety_nine_percentile",
                "type": "FLOAT",
                "description": "The value from this monitor for which 99 per cent of the rest of the measured values for the year are equal to or less than.",
                "mode": "NULLABLE",
            },
            {
                "name": "ninety_eight_percentile",
                "type": "FLOAT",
                "description": "The value from this monitor for which 98 per cent of the rest of the measured values for the year are equal to or less than.",
                "mode": "NULLABLE",
            },
            {
                "name": "ninety_five_percentile",
                "type": "FLOAT",
                "description": "The value from this monitor for which 95 per cent of the rest of the measured values for the year are equal to or less than.",
                "mode": "NULLABLE",
            },
            {
                "name": "ninety_percentile",
                "type": "FLOAT",
                "description": "The value from this monitor for which 90 per cent of the rest of the measured values for the year are equal to or less than.",
                "mode": "NULLABLE",
            },
            {
                "name": "seventy_five_percentile",
                "type": "FLOAT",
                "description": "The value from this monitor for which 75 per cent of the rest of the measured values for the year are equal to or less than.",
                "mode": "NULLABLE",
            },
            {
                "name": "fifty_percentile",
                "type": "FLOAT",
                "description": "The value from this monitor for which 50 per cent of the rest of the measured values for the year are equal to or less than (i.e., the median).",
                "mode": "NULLABLE",
            },
            {
                "name": "ten_percentile",
                "type": "FLOAT",
                "description": "The value from this monitor for which 10 per cent of the rest of the measured values for the year are equal to or less than.",
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
