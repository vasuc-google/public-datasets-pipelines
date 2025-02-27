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
    dag_id="google_political_ads.creative_stats",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="@daily",
    catchup=False,
    default_view="graph",
) as dag:

    # Run CSV transform within kubernetes pod
    creative_stats_transform_csv = kubernetes_pod.KubernetesPodOperator(
        task_id="creative_stats_transform_csv",
        startup_timeout_seconds=600,
        name="creative_stats",
        namespace="composer",
        service_account_name="datasets",
        image_pull_policy="Always",
        image="{{ var.json.google_political_ads.container_registry.run_csv_transform_kub }}",
        env_vars={
            "SOURCE_URL": "https://storage.googleapis.com/transparencyreport/google-political-ads-transparency-bundle.zip",
            "SOURCE_FILE": "files/data.zip",
            "FILE_NAME": "google-political-ads-transparency-bundle/google-political-ads-creative-stats.csv",
            "TARGET_FILE": "files/data_output.csv",
            "TARGET_GCS_BUCKET": "{{ var.value.composer_bucket }}",
            "TARGET_GCS_PATH": "data/google_political_ads/creative_stats/data_output.csv",
            "PIPELINE_NAME": "creative_stats",
            "CSV_HEADERS": '["ad_id","ad_url","ad_type","regions","advertiser_id","advertiser_name","ad_campaigns_list","date_range_start","date_range_end","num_of_days","impressions","spend_usd","first_served_timestamp","last_served_timestamp","age_targeting","gender_targeting","geo_targeting_included","geo_targeting_excluded","spend_range_min_usd","spend_range_max_usd","spend_range_min_eur","spend_range_max_eur","spend_range_min_inr","spend_range_max_inr","spend_range_min_bgn","spend_range_max_bgn","spend_range_min_hrk","spend_range_max_hrk","spend_range_min_czk","spend_range_max_czk","spend_range_min_dkk","spend_range_max_dkk","spend_range_min_huf","spend_range_max_huf","spend_range_min_pln","spend_range_max_pln","spend_range_min_ron","spend_range_max_ron","spend_range_min_sek","spend_range_max_sek","spend_range_min_gbp","spend_range_max_gbp","spend_range_min_nzd","spend_range_max_nzd"]',
            "RENAME_MAPPINGS": '{"Ad_ID": "ad_id","Ad_URL": "ad_url","Ad_Type": "ad_type","Regions": "regions","Advertiser_ID": "advertiser_id","Advertiser_Name": "advertiser_name","Ad_Campaigns_List": "ad_campaigns_list","Date_Range_Start": "date_range_start","Date_Range_End": "date_range_end","Num_of_Days": "num_of_days","Impressions": "impressions","Spend_USD": "spend_usd","Spend_Range_Min_USD": "spend_range_min_usd","Spend_Range_Max_USD": "spend_range_max_usd","Spend_Range_Min_EUR": "spend_range_min_eur","Spend_Range_Max_EUR": "spend_range_max_eur","Spend_Range_Min_INR": "spend_range_min_inr","Spend_Range_Max_INR": "spend_range_max_inr","Spend_Range_Min_BGN": "spend_range_min_bgn","Spend_Range_Max_BGN": "spend_range_max_bgn","Spend_Range_Min_HRK": "spend_range_min_hrk","Spend_Range_Max_HRK": "spend_range_max_hrk","Spend_Range_Min_CZK": "spend_range_min_czk","Spend_Range_Max_CZK": "spend_range_max_czk","Spend_Range_Min_DKK": "spend_range_min_dkk","Spend_Range_Max_DKK": "spend_range_max_dkk","Spend_Range_Min_HUF": "spend_range_min_huf","Spend_Range_Max_HUF": "spend_range_max_huf","Spend_Range_Min_PLN": "spend_range_min_pln","Spend_Range_Max_PLN": "spend_range_max_pln","Spend_Range_Min_RON": "spend_range_min_ron","Spend_Range_Max_RON": "spend_range_max_ron","Spend_Range_Min_SEK": "spend_range_min_sek","Spend_Range_Max_SEK": "spend_range_max_sek","Spend_Range_Min_GBP": "spend_range_min_gbp","Spend_Range_Max_GBP": "spend_range_max_gbp","Spend_Range_Min_NZD": "spend_range_min_nzd","Spend_Range_Max_NZD": "spend_range_max_nzd","Age_Targeting": "age_targeting","Gender_Targeting": "gender_targeting","Geo_Targeting_Included": "geo_targeting_included","Geo_Targeting_Excluded": "geo_targeting_excluded","First_Served_Timestamp": "first_served_timestamp","Last_Served_Timestamp": "last_served_timestamp"}',
        },
        resources={
            "request_memory": "8G",
            "request_cpu": "2",
            "request_ephemeral_storage": "10G",
        },
    )

    # Task to load CSV data to a BigQuery table
    load_creative_stats_to_bq = gcs_to_bigquery.GCSToBigQueryOperator(
        task_id="load_creative_stats_to_bq",
        bucket="{{ var.value.composer_bucket }}",
        source_objects=["data/google_political_ads/creative_stats/data_output.csv"],
        source_format="CSV",
        destination_project_dataset_table="google_political_ads.creative_stats",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {
                "name": "ad_id",
                "type": "string",
                "description": "Unique id for a specific election ad.",
                "mode": "nullable",
            },
            {
                "name": "ad_url",
                "type": "string",
                "description": "URL to view the election ad in the election Advertising on Google report.",
                "mode": "nullable",
            },
            {
                "name": "ad_type",
                "type": "string",
                "description": "The type of the ad. Can be TEXT VIDEO or IMAGE.",
                "mode": "nullable",
            },
            {
                "name": "regions",
                "type": "string",
                "description": "The regions that this ad is verified for or were served in.",
                "mode": "nullable",
            },
            {
                "name": "advertiser_id",
                "type": "string",
                "description": "ID of the advertiser who purchased the ad.",
                "mode": "nullable",
            },
            {
                "name": "advertiser_name",
                "type": "string",
                "description": "Name of advertiser.",
                "mode": "nullable",
            },
            {
                "name": "ad_campaigns_list",
                "type": "string",
                "description": "IDs of all election ad campaigns that included the ad.",
                "mode": "nullable",
            },
            {
                "name": "date_range_start",
                "type": "date",
                "description": "First day a election ad ran and had an impression.",
                "mode": "nullable",
            },
            {
                "name": "date_range_end",
                "type": "date",
                "description": "Most recent day a election ad ran and had an impression.",
                "mode": "nullable",
            },
            {
                "name": "num_of_days",
                "type": "integer",
                "description": "Total number of days a election ad ran and had an impression.",
                "mode": "nullable",
            },
            {
                "name": "impressions",
                "type": "string",
                "description": "Number of impressions for the election ad. Impressions are grouped into several buckets ≤ 10k 10k-100k 100k-1M 1M-10M > 10M.",
                "mode": "nullable",
            },
            {
                "name": "spend_usd",
                "type": "string",
                "description": "[DEPRECATED] This field is deprecated in favor of specifying the lower and higher spend bucket bounds in separate Spend_Range_Min and Spend_Range_Max columns.",
                "mode": "nullable",
            },
            {
                "name": "first_served_timestamp",
                "type": "timestamp",
                "description": "The timestamp of the earliest impression for this ad.",
                "mode": "nullable",
            },
            {
                "name": "last_served_timestamp",
                "type": "timestamp",
                "description": "The timestamp of the most recent impression for this ad.",
                "mode": "nullable",
            },
            {
                "name": "age_targeting",
                "type": "string",
                "description": "Age ranges included in the ad's targeting",
                "mode": "nullable",
            },
            {
                "name": "gender_targeting",
                "type": "string",
                "description": "Genders included in the ad's targeting.",
                "mode": "nullable",
            },
            {
                "name": "geo_targeting_included",
                "type": "string",
                "description": "Geographic locations included in the ad's targeting.",
                "mode": "nullable",
            },
            {
                "name": "geo_targeting_excluded",
                "type": "string",
                "description": "Geographic locations excluded in the ad's targeting.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_min_usd",
                "type": "integer",
                "description": "Lower bound of the amount in USD spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_max_usd",
                "type": "integer",
                "description": "Upper bound of the amount in USD spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_min_eur",
                "type": "integer",
                "description": "Lower bound of the amount in EUR spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_max_eur",
                "type": "integer",
                "description": "Upper bound of the amount in EUR spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_min_inr",
                "type": "integer",
                "description": "Lower bound of the amount in INR spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_max_inr",
                "type": "integer",
                "description": "Upper bound of the amount in INR spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_min_bgn",
                "type": "integer",
                "description": "Lower bound of the amount in BGN spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_max_bgn",
                "type": "integer",
                "description": "Upper bound of the amount in BGN spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_min_hrk",
                "type": "integer",
                "description": "Lower bound of the amount in HRK spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_max_hrk",
                "type": "integer",
                "description": "Upper bound of the amount in HRK spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_min_czk",
                "type": "integer",
                "description": "Lower bound of the amount in CZK spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_max_czk",
                "type": "integer",
                "description": "Upper bound of the amount in CZK spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_min_dkk",
                "type": "integer",
                "description": "Lower bound of the amount in DKK spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_max_dkk",
                "type": "integer",
                "description": "Upper bound of the amount in DKK spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_min_huf",
                "type": "integer",
                "description": "Lower bound of the amount in HUF spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_max_huf",
                "type": "integer",
                "description": "Upper bound of the amount in HUF spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_min_pln",
                "type": "integer",
                "description": "Lower bound of the amount in PLN spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_max_pln",
                "type": "integer",
                "description": "Upper bound of the amount in PLN spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_min_ron",
                "type": "integer",
                "description": "Lower bound of the amount in RON spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_max_ron",
                "type": "integer",
                "description": "Upper bound of the amount in RON spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_min_sek",
                "type": "integer",
                "description": "Lower bound of the amount in SEK spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_max_sek",
                "type": "integer",
                "description": "Upper bound of the amount in SEK spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_min_gbp",
                "type": "integer",
                "description": "Lower bound of the amount in GBP spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_max_gbp",
                "type": "integer",
                "description": "Upper bound of the amount in GBP spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_min_nzd",
                "type": "integer",
                "description": "Lower bound of the amount in NZD spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
            {
                "name": "spend_range_max_nzd",
                "type": "integer",
                "description": "Upper bound of the amount in NZD spent by the advertiser on the election ad.",
                "mode": "nullable",
            },
        ],
    )

    creative_stats_transform_csv >> load_creative_stats_to_bq
