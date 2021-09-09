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
    dag_id="census_bureau_international.country_names_area",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="@daily",
    catchup=False,
    default_view="graph",
) as dag:

    # Run CSV transform within kubernetes pod
    census_bureau_international_country_names_area_transform_csv = kubernetes_pod.KubernetesPodOperator(
        task_id="census_bureau_international_country_names_area_transform_csv",
        startup_timeout_seconds=600,
        name="country_names_area",
        namespace="default",
        image_pull_policy="Always",
        image="{{ var.json.census_bureau_international.container_registry.run_csv_transform_kub_country_names_area}}",
        env_vars={
            "SOURCE_URL": "https://storage.cloud.google.com/pdp-feeds-staging/Census/idbzip/IDBextCTYS.csv",
            "SOURCE_FILE": "files/data.csv",
            "TARGET_FILE": "files/data_output.csv",
            "TARGET_GCS_BUCKET": "{{ var.json.shared.composer_bucket }}",
            "TARGET_GCS_PATH": "data/census_bureau_international/country_names_area/data_output.csv",
        },
        resources={"limit_memory": "4G", "limit_cpu": "1"},
    )

    # Task to load CSV data to a BigQuery table
    load_census_bureau_international_country_names_area_to_bq = gcs_to_bigquery.GCSToBigQueryOperator(
        task_id="load_census_bureau_international_country_names_area_to_bq",
        bucket="{{ var.json.shared.composer_bucket }}",
        source_objects=[
            "data/census_bureau_international/country_names_area/data_output.csv"
        ],
        source_format="CSV",
        destination_project_dataset_table="census_bureau_international.country_names_area",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {"name": "country_code", "type": "STRING", "mode": "required"},
            {"name": "country_name", "type": "STRING", "mode": "NULLABLE"},
            {"name": "country_area", "type": "FLOAT", "mode": "NULLABLE"},
        ],
    )

    census_bureau_international_country_names_area_transform_csv >> load_census_bureau_international_country_names_area_to_bq
