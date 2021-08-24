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
from airflow.contrib.operators import gcs_to_bq, kubernetes_pod_operator

default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "start_date": "2021-03-01",
}


with DAG(
    dag_id="cloud_storage_geo_index.landsat_index",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="@daily",
    catchup=False,
    default_view="graph",
) as dag:

    # Run CSV transform within kubernetes pod
    cloud_storage_geo_index_landsat_transform_csv = kubernetes_pod_operator.KubernetesPodOperator(
        task_id="cloud_storage_geo_index_landsat_transform_csv",
        startup_timeout_seconds=600,
        name="cloud_storage_geo_index",
        namespace="default",
        image_pull_policy="Always",
        image="{{ var.json.cloud_storage_geo_index.container_registry.run_csv_transform_kub_landsat_index }}",
        env_vars={
            "SOURCE_URL": "gs://gcp-public-data-landsat/index.csv.gz",
            "SOURCE_FILE": "files/data.csv",
            "TARGET_FILE": "files/data_output.csv",
            "TARGET_GCS_BUCKET": "{{ var.json.shared.composer_bucket }}",
            "TARGET_GCS_PATH": "data/cloud_storage_geo_index/landsat_index/data_output.csv",
        },
        resources={"limit_memory": "4G", "limit_cpu": "1"},
    )

    # Task to load CSV data to a BigQuery table
    load_cloud_storage_geo_index_landsat_to_bq = (
        gcs_to_bq.GoogleCloudStorageToBigQueryOperator(
            task_id="load_cloud_storage_geo_index_landsat_to_bq",
            bucket="{{ var.json.shared.composer_bucket }}",
            source_objects=[
                "data/cloud_storage_geo_index/landsat_index/data_output.csv"
            ],
            source_format="CSV",
            destination_project_dataset_table="cloud_storage_geo_index.landsat_index",
            skip_leading_rows=1,
            write_disposition="WRITE_TRUNCATE",
            schema_fields=[
                {"name": "scene_id", "type": "STRING", "mode": "required"},
                {"name": "product_id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "spacecraft_id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "sensor_id", "type": "STRING", "mode": "NULLABLE"},
                {"name": "date_acquired", "type": "DATE", "mode": "NULLABLE"},
                {"name": "sensing_time", "type": "TIMESTAMP", "mode": "NULLABLE"},
                {"name": "collection_number", "type": "STRING", "mode": "NULLABLE"},
                {"name": "collection_category", "type": "STRING", "mode": "NULLABLE"},
                {"name": "data_type", "type": "STRING", "mode": "NULLABLE"},
                {"name": "wrs_path", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "wrs_row", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "cloud_cover", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "north_lat", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "south_lat", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "west_lon", "type": "FLOAT", "mode": "NULLABLE"},
                {"name": "east_lon", "type": "FLAOT", "mode": "NULLABLE"},
                {"name": "total_size", "type": "INTEGER", "mode": "NULLABLE"},
                {"name": "base_url", "type": "STRING", "mode": "NULLABLE"},
            ],
        )
    )

    cloud_storage_geo_index_landsat_transform_csv >> load_cloud_storage_geo_index_landsat_to_bq
