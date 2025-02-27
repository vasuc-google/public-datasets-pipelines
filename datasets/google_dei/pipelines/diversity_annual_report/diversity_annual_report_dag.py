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
from airflow.providers.google.cloud.transfers import gcs_to_bigquery

default_args = {
    "owner": "Google",
    "depends_on_past": False,
    "start_date": "2021-05-01",
}


with DAG(
    dag_id="google_dei.diversity_annual_report",
    default_args=default_args,
    max_active_runs=1,
    schedule_interval="@once",
    catchup=False,
    default_view="graph",
) as dag:

    # Task to load CSV data to a BigQuery table
    load_intersectional_attrition_to_bq = gcs_to_bigquery.GCSToBigQueryOperator(
        task_id="load_intersectional_attrition_to_bq",
        bucket="{{ var.json.google_dei.storage_bucket }}",
        source_objects=["DAR/intersectional_attrition.csv"],
        source_format="CSV",
        destination_project_dataset_table="google_dei.dar_intersectional_attrition",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {
                "name": "workforce",
                "description": "Overall and sub-categories",
                "type": "string",
                "mode": "required",
            },
            {
                "name": "report_year",
                "description": "The year the report was published",
                "type": "integer",
                "mode": "required",
            },
            {
                "name": "gender_us",
                "description": "Gender of Googlers in the U.S.",
                "type": "string",
                "mode": "required",
            },
            {
                "name": "race_asian",
                "description": "The attrition index score of Googlers in the U.S. who identify as Asian",
                "type": "integer",
                "mode": "nullable",
            },
            {
                "name": "race_black",
                "description": "The attrition index score of Googlers in the U.S. who identify as Black",
                "type": "integer",
                "mode": "nullable",
            },
            {
                "name": "race_hispanic_latinx",
                "description": "The attrition index score of Googlers in the U.S. who identify as Hispanic or Latinx",
                "type": "integer",
                "mode": "nullable",
            },
            {
                "name": "race_native_american",
                "description": "The attrition index score of Googlers in the U.S. who identify as Native American",
                "type": "integer",
                "mode": "nullable",
            },
            {
                "name": "race_white",
                "description": "The attrition index score of Googlers in the U.S. who identify as White",
                "type": "integer",
                "mode": "nullable",
            },
        ],
    )

    # Task to load CSV data to a BigQuery table
    load_intersectional_hiring_to_bq = gcs_to_bigquery.GCSToBigQueryOperator(
        task_id="load_intersectional_hiring_to_bq",
        bucket="{{ var.json.google_dei.storage_bucket }}",
        source_objects=["DAR/intersectional_hiring.csv"],
        source_format="CSV",
        destination_project_dataset_table="google_dei.dar_intersectional_hiring",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {
                "name": "workforce",
                "description": "Overall and sub-categories",
                "type": "string",
                "mode": "required",
            },
            {
                "name": "report_year",
                "description": "The year the report was published",
                "type": "integer",
                "mode": "required",
            },
            {
                "name": "gender_us",
                "description": "Gender of Googlers hired in the U.S.",
                "type": "string",
                "mode": "required",
            },
            {
                "name": "race_asian",
                "description": "The percentage of Googlers hired in the U.S. who identify as Asian",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "race_black",
                "description": "The percentage of Googlers hired in the U.S. who identify as Black",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "race_hispanic_latinx",
                "description": "The percentage of Googlers hired in the U.S. who identify as Hispanic or Latinx",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "race_native_american",
                "description": "The percentage of Googlers hired in the U.S. who identify as Native American",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "race_white",
                "description": "The percentage of Googlers hired in the U.S. who identify as White",
                "type": "float",
                "mode": "nullable",
            },
        ],
    )

    # Task to load CSV data to a BigQuery table
    load_intersectional_representation_to_bq = gcs_to_bigquery.GCSToBigQueryOperator(
        task_id="load_intersectional_representation_to_bq",
        bucket="{{ var.json.google_dei.storage_bucket }}",
        source_objects=["DAR/intersectional_representation.csv"],
        source_format="CSV",
        destination_project_dataset_table="google_dei.dar_intersectional_representation",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {
                "name": "workforce",
                "description": "Overall and sub-categories",
                "type": "string",
                "mode": "required",
            },
            {
                "name": "report_year",
                "description": "The year the report was published",
                "type": "integer",
                "mode": "required",
            },
            {
                "name": "gender_us",
                "description": "Gender of Googlers in the U.S.",
                "type": "string",
                "mode": "required",
            },
            {
                "name": "race_asian",
                "description": "The percentage of Googlers in the U.S. who identify as Asian",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "race_black",
                "description": "The percentage of Googlers in the U.S. who identify as Black",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "race_hispanic_latinx",
                "description": "The percentage of Googlers in the U.S. who identify as Hispanic or Latinx",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "race_native_american",
                "description": "The percentage of Googlers in the U.S. who identify as Native American",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "race_white",
                "description": "The percentage of Googlers in the U.S. who identify as White",
                "type": "float",
                "mode": "nullable",
            },
        ],
    )

    # Task to load CSV data to a BigQuery table
    load_non_intersectional_representation_to_bq = gcs_to_bigquery.GCSToBigQueryOperator(
        task_id="load_non_intersectional_representation_to_bq",
        bucket="{{ var.json.google_dei.storage_bucket }}",
        source_objects=["DAR/non_intersectional_representation.csv"],
        source_format="CSV",
        destination_project_dataset_table="google_dei.dar_non_intersectional_representation",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {
                "name": "workforce",
                "description": "Overall and sub-categories",
                "type": "string",
                "mode": "required",
            },
            {
                "name": "report_year",
                "description": "The year the report was published",
                "type": "integer",
                "mode": "required",
            },
            {
                "name": "race_asian",
                "description": "The percentage of Googlers in the U.S. who identify as Asian",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "race_black",
                "description": "The percentage of Googlers in the U.S. who identify as Black",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "race_hispanic_latinx",
                "description": "The percentage of Googlers in the U.S. who identify as Hispanic or Latinx",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "race_native_american",
                "description": "The percentage of Googlers in the U.S. who identify as Native American",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "race_white",
                "description": "The percentage of Googlers in the U.S. who identify as White",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "gender_us_female",
                "description": "The percentage of Googlers in the U.S. who identify as female",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "gender_us_male",
                "description": "The percentage of Googlers in the U.S. who identify as male",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "gender_global_female",
                "description": "The percentage of global Googlers who identify as female",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "gender_global_male",
                "description": "The percentage of global Googlers who identify as male",
                "type": "float",
                "mode": "nullable",
            },
        ],
    )

    # Task to load CSV data to a BigQuery table
    load_non_intersectional_attrition_to_bq = gcs_to_bigquery.GCSToBigQueryOperator(
        task_id="load_non_intersectional_attrition_to_bq",
        bucket="{{ var.json.google_dei.storage_bucket }}",
        source_objects=["DAR/non_intersectional_attrition.csv"],
        source_format="CSV",
        destination_project_dataset_table="google_dei.dar_non_intersectional_attrition",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {
                "name": "workforce",
                "description": "Overall and sub-categories",
                "type": "string",
                "mode": "required",
            },
            {
                "name": "report_year",
                "description": "The year the report was published",
                "type": "integer",
                "mode": "required",
            },
            {
                "name": "race_asian",
                "description": "The attrition index score of Googlers in the U.S. who identify as Asian",
                "type": "integer",
                "mode": "nullable",
            },
            {
                "name": "race_black",
                "description": "The attrition index score of Googlers in the U.S. who identify as Black",
                "type": "integer",
                "mode": "nullable",
            },
            {
                "name": "race_hispanic_latinx",
                "description": "The attrition index score of Googlers in the U.S. who identify as Hispanic or Latinx",
                "type": "integer",
                "mode": "nullable",
            },
            {
                "name": "race_native_american",
                "description": "The attrition index score of Googlers in the U.S. who identify as Native American",
                "type": "integer",
                "mode": "nullable",
            },
            {
                "name": "race_white",
                "description": "The attrition index score of Googlers in the U.S. who identify as White",
                "type": "integer",
                "mode": "nullable",
            },
            {
                "name": "gender_us_female",
                "description": "The attrition index score of Googlers in the U.S. who are female",
                "type": "integer",
                "mode": "nullable",
            },
            {
                "name": "gender_us_male",
                "description": "The attrition index score of Googlers in the U.S. who are male",
                "type": "integer",
                "mode": "nullable",
            },
            {
                "name": "gender_global_female",
                "description": "The attrition index score of global Googlers who are female",
                "type": "integer",
                "mode": "nullable",
            },
            {
                "name": "gender_global_male",
                "description": "The attrition index score of global Googlers who are male",
                "type": "integer",
                "mode": "nullable",
            },
        ],
    )

    # Task to load CSV data to a BigQuery table
    load_non_intersectional_hiring_to_bq = gcs_to_bigquery.GCSToBigQueryOperator(
        task_id="load_non_intersectional_hiring_to_bq",
        bucket="{{ var.json.google_dei.storage_bucket }}",
        source_objects=["DAR/non_intersectional_hiring.csv"],
        source_format="CSV",
        destination_project_dataset_table="google_dei.dar_non_intersectional_hiring",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {
                "name": "workforce",
                "description": "Overall and sub-categories",
                "type": "string",
                "mode": "required",
            },
            {
                "name": "report_year",
                "description": "The year the report was published",
                "type": "integer",
                "mode": "required",
            },
            {
                "name": "race_asian",
                "description": "The percentage of Googlers hired in the U.S. who identify as Asian",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "race_black",
                "description": "The percentage of Googlers hired in the U.S. who identify as Black",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "race_hispanic_latinx",
                "description": "The percentage of Googlers hired in the U.S. who identify as Hispanic or Latinx",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "race_native_american",
                "description": "The percentage of Googlers hired in the U.S. who identify as Native American",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "race_white",
                "description": "The percentage of Googlers hired in the U.S. who identify as White",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "gender_us_female",
                "description": "The percentage of Googlers hired in the U.S. who are female",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "gender_us_male",
                "description": "The percentage of Googlers hired in the U.S. who are male",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "gender_global_female",
                "description": "The percentage of global Googlers hired who are female",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "gender_global_male",
                "description": "The percentage of global Googlers hired who are male",
                "type": "float",
                "mode": "nullable",
            },
        ],
    )

    # Task to load CSV data to a BigQuery table
    load_region_non_intersectional_attrition_to_bq = gcs_to_bigquery.GCSToBigQueryOperator(
        task_id="load_region_non_intersectional_attrition_to_bq",
        bucket="{{ var.json.google_dei.storage_bucket }}",
        source_objects=["DAR/region_non_intersectional_attrition.csv"],
        source_format="CSV",
        destination_project_dataset_table="google_dei.dar_region_non_intersectional_attrition",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {
                "name": "workforce",
                "description": "Overall and sub-categories",
                "type": "string",
                "mode": "required",
            },
            {
                "name": "region",
                "description": "Region",
                "type": "string",
                "mode": "required",
            },
            {
                "name": "report_year",
                "description": "The year the report was published",
                "type": "integer",
                "mode": "required",
            },
            {
                "name": "gender_female",
                "description": "The attrition index score of Googlers in the region who are female",
                "type": "integer",
                "mode": "nullable",
            },
            {
                "name": "gender_male",
                "description": "The attrition index score of Googlers in the region who are male",
                "type": "integer",
                "mode": "nullable",
            },
        ],
    )

    # Task to load CSV data to a BigQuery table
    load_region_non_intersectional_hiring_to_bq = gcs_to_bigquery.GCSToBigQueryOperator(
        task_id="load_region_non_intersectional_hiring_to_bq",
        bucket="{{ var.json.google_dei.storage_bucket }}",
        source_objects=["DAR/region_non_intersectional_hiring.csv"],
        source_format="CSV",
        destination_project_dataset_table="google_dei.dar_region_non_intersectional_hiring",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {
                "name": "workforce",
                "description": "Overall and sub-categories",
                "type": "string",
                "mode": "required",
            },
            {
                "name": "region",
                "description": "Region",
                "type": "string",
                "mode": "required",
            },
            {
                "name": "report_year",
                "description": "The year the report was published",
                "type": "integer",
                "mode": "required",
            },
            {
                "name": "gender_female",
                "description": "The percentage of Googlers in the region who are female",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "gender_male",
                "description": "The percentage of Googlers in the region who are male",
                "type": "float",
                "mode": "nullable",
            },
        ],
    )

    # Task to load CSV data to a BigQuery table
    load_region_non_intersectional_representation_to_bq = gcs_to_bigquery.GCSToBigQueryOperator(
        task_id="load_region_non_intersectional_representation_to_bq",
        bucket="{{ var.json.google_dei.storage_bucket }}",
        source_objects=["DAR/region_non_intersectional_representation.csv"],
        source_format="CSV",
        destination_project_dataset_table="google_dei.dar_region_non_intersectional_representation",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {
                "name": "workforce",
                "description": "Overall and sub-categories",
                "type": "string",
                "mode": "required",
            },
            {
                "name": "region",
                "description": "Region",
                "type": "string",
                "mode": "required",
            },
            {
                "name": "report_year",
                "description": "The year the report was published",
                "type": "integer",
                "mode": "required",
            },
            {
                "name": "race_asian",
                "description": "The percentage of Googlers in the region who identify as Asian",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "race_black_african",
                "description": "The percentage of Googlers in the region who identify as Black African",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "race_hispanic_latino_latinx",
                "description": "The percentage of Googlers in the region who identify as Hispanic or Latino or Latinx",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "race_indigenous",
                "description": "The percentage of Googlers in the region who identify as Indigenous",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "race_mena",
                "description": "The percentage of Googlers in the region who identify as Middle Eastern or North African",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "race_white_european",
                "description": "The percentage of Googlers in the region who identify as White European",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "gender_female",
                "description": "The percentage of Googlers in the region who are female",
                "type": "float",
                "mode": "nullable",
            },
            {
                "name": "gender_male",
                "description": "The percentage of Googlers in the region who are male",
                "type": "float",
                "mode": "nullable",
            },
        ],
    )

    # Task to load CSV data to a BigQuery table
    load_self_id_to_bq = gcs_to_bigquery.GCSToBigQueryOperator(
        task_id="load_self_id_to_bq",
        bucket="{{ var.json.google_dei.storage_bucket }}",
        source_objects=["DAR/self_id.csv"],
        source_format="CSV",
        destination_project_dataset_table="google_dei.dar_self_id",
        skip_leading_rows=1,
        write_disposition="WRITE_TRUNCATE",
        schema_fields=[
            {
                "name": "workforce",
                "description": "Self-identification category. lgbtq: Global Googlers who self-identified as LGBQ+ and/or Trans+; disability: Global Googlers who self-identified as having a disability; military: Global Googlers who self-identified as being or having been members of the military; nonbinary: Global Googlers who self-identified as non-binary",
                "type": "string",
                "mode": "required",
            },
            {
                "name": "report_year",
                "description": "The year the report was published",
                "type": "integer",
                "mode": "required",
            },
            {
                "name": "global",
                "description": 'The percentage of Googlers who identify as being part of the self-identification category (i.e., "workforce" type)',
                "type": "float",
                "mode": "nullable",
            },
        ],
    )

    load_intersectional_attrition_to_bq
    load_intersectional_hiring_to_bq
    load_intersectional_representation_to_bq
    load_non_intersectional_attrition_to_bq
    load_non_intersectional_hiring_to_bq
    load_non_intersectional_representation_to_bq
    load_region_non_intersectional_attrition_to_bq
    load_region_non_intersectional_hiring_to_bq
    load_region_non_intersectional_representation_to_bq
    load_self_id_to_bq
