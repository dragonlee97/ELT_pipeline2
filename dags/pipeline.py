import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCreateEmptyDatasetOperator,
    BigQueryCreateEmptyTableOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)
from airflow.utils.trigger_rule import TriggerRule
from google.cloud import bigquery

from helpers.utils import (
    extract_from_api_upload_to_gcs,
    read_from_minio_load_to_temp_table,
)

dag_id = "airbnb_concierge"
dag_folder = os.path.dirname(os.path.realpath(__file__))
with open(os.path.join(dag_folder, "config.json"), "r") as file:
    params = json.load(file)


default_args = {
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15),
    "depends_on_past": False,
}

dag = DAG(
    dag_id,
    start_date=datetime(2024, 1, 2),
    default_args=default_args,
    description="This dag extracts data from Opendatasoft API and Minio to gcs, then loads the json to a temp table and finally create/ingest to table",
    schedule_interval="0 12 * * *",
    max_active_runs=1,
    max_active_tasks=1,
    template_searchpath=os.path.join(dag_folder, "sql"),
)

with dag:
    start = EmptyOperator(task_id="processing_start")
    # Create raw dataset for both listings and customers source data
    create_raw_dataset_if_not_exists = BigQueryCreateEmptyDatasetOperator(
        task_id="create_raw_dataset_if_not_exists",
        project_id=params["GCP_PROJECT_ID"],
        dataset_id=params["GCP_RAW_DATASET"],
        if_exists="ignore",
    )

    # Save airbnb api data to the GCS bucket in NDJSON format. Retry parameters are specified for this task
    airbnb_listings_to_gcs = PythonOperator(
        task_id="airbnb_listings_to_gcs",
        python_callable=extract_from_api_upload_to_gcs,
        op_kwargs={"gcs_bucket": params["GCS_BUCKET"]},
        retries=10,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=60),
    )

    # Load the json file from GCS to Bigquery. Schema-on-read mode.
    load_airbnb_listings_to_bq = GCSToBigQueryOperator(
        task_id="load_airbnb_listings_to_bq",
        gcp_conn_id="google_cloud_default",
        bucket=params["GCS_BUCKET"],
        source_objects=["20240103/airbnb_listings.json"],
        # source_objects=["{{ ti.xcom_pull('airbnb_listings_to_gcs') }}"],
        destination_project_dataset_table=params["AIRBNB_LISTINGS_RAW_TABLE"],
        write_disposition="WRITE_TRUNCATE",
        source_format="NEWLINE_DELIMITED_JSON",
        schema_fields=json.load(
            open("{}/schema/airbnb_listing_raw.json".format(dag_folder))
        ),
        ignore_unknown_values=True,
    )

    #
    create_companies_table_if_not_exists = BigQueryCreateEmptyTableOperator(
        task_id="create_companies_table_if_not_exists",
        project_id=params["GCP_PROJECT_ID"],
        dataset_id=params["GCP_RAW_DATASET"],
        table_id="companies",
        gcp_conn_id="google_cloud_default",
        schema_fields=json.load(
            open("{}/schema/companies_raw.json".format(dag_folder))
        ),
        if_exists="ignore",
    )

    # Load today's csv file to a staging table, ready for updating raw source table. If there is no csv today, go to datamart tasks directly
    companies_to_temp_bigquery = BranchPythonOperator(
        task_id="companies_to_temp_bigquery",
        python_callable=read_from_minio_load_to_temp_table,
        op_kwargs={
            "minio_bucket_name": params["MINIO_BUCKET"],
            "bigquery_temp_table": params["COMPANIES_TEMP_TABLE"],
        },
        retries=10,
        retry_delay=timedelta(minutes=5),
        execution_timeout=timedelta(minutes=60),
    )

    # Update raw companies table using the staging table with MERGE sql statement
    load_temp_companies_to_bq = BigQueryInsertJobOperator(
        task_id="load_temp_companies_to_bq",
        project_id=params["GCP_PROJECT_ID"],
        configuration={
            "query": {
                "query": "{% include 'sql/merge_into_companies.sql' %}",
                "useLegacySql": False,
                "priority": "BATCH",
            },
        },
        params={
            "bigquery_temp_table": params["COMPANIES_TEMP_TABLE"],
            "customers_table": params["COMPANIES_RAW_TABLE"],
        },
    )

    # Create dataset and table using DDL queries. Use dbt materialization can help skip this creation task
    create_monitoring_datamart_if_not_exists = BigQueryInsertJobOperator(
        task_id="create_monitoring_datamart_if_not_exists",
        trigger_rule=TriggerRule.NONE_FAILED,
        project_id=params["GCP_PROJECT_ID"],
        configuration={
            "query": {
                "query": "{% include 'sql/create_datamart.sql' %}",
                "useLegacySql": False,
            },
        },
        params={
            "dataset_id": params["GCP_DATAMART_DATASET"],
            "destination_table_id": params["FINAL_DATAMART_TABLE"],
        },
    )

    # DML SQL to populate the monitoring datamart
    compute_final_datamart = BigQueryInsertJobOperator(
        task_id="compute_final_datamart",
        project_id=params["GCP_PROJECT_ID"],
        configuration={
            "query": {
                "query": "{% include 'sql/compute_datamart.sql' %}",
                "useLegacySql": False,
                "priority": "BATCH",
                "destinationTable": {
                    "projectId": params["GCP_PROJECT_ID"],
                    "datasetId": params["GCP_DATAMART_DATASET"],
                    "tableId": "monitoring${{ds_nodash}}",
                },
                "writeDisposition": "WRITE_TRUNCATE",
            },
        },
        params={
            "listings_table": params["AIRBNB_LISTINGS_RAW_TABLE"],
            "customers_table": params["COMPANIES_RAW_TABLE"],
        },
    )

    start >> create_raw_dataset_if_not_exists >> [
        airbnb_listings_to_gcs,
        create_companies_table_if_not_exists,
    ]
    airbnb_listings_to_gcs >> load_airbnb_listings_to_bq
    create_companies_table_if_not_exists >> companies_to_temp_bigquery >> [
        load_temp_companies_to_bq,
        create_monitoring_datamart_if_not_exists,
    ]
    [
        companies_to_temp_bigquery,
        load_airbnb_listings_to_bq,
        load_temp_companies_to_bq,
    ] >> create_monitoring_datamart_if_not_exists >> compute_final_datamart
