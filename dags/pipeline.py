import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.latest_only import LatestOnlyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator

from exercise2.utils import extract_from_api_upload_to_gcs

dag_id = ""
dag_folder = os.path.dirname(os.path.realpath(__file__))
with open(os.path.join(dag_folder, "config.json"), "r") as file:
    params = json.load(file)
GCP_PROJECT_ID = params["GCP_PROJECT_ID"]
GCS_BUCKET = params["GCS_BUCKET"]
FINAL_TABLE = params["FINAL_TABLE"]
TEMP_TABLE = params["TEMP_TABLE"]

default_args = {
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=15),
    "depends_on_past": False,
}

dag = DAG(
    dag_id,
    start_date=datetime(2024, 1, 1),
    default_args=default_args,
    description="This dag extracts data from the API and pushes jsons to gcs, then loads the json to a temp table and finally create/ingest to table",
    schedule_interval="0 12 * * *",
    max_active_runs=1,
    max_active_tasks=1,
    template_searchpath=os.path.join(dag_folder, "sql"),
)

with dag:
    # Using LatestOnlyOperator, as there is no need run on previous days
    latest_only = LatestOnlyOperator(task_id="latest_only")
    # The function calls the api the save the data to the GCS bucket. Retry parameters are specified for this task
    api_to_gcs = PythonOperator(
        task_id="api_to_gcs",
        python_callable=extract_from_api_upload_to_gcs,
        op_kwargs={"gcs_bucket": GCS_BUCKET},
        retries=10,
        retry_delay=timedelta(minutes=2),
        execution_timeout=timedelta(minutes=60),
    )
    # Load the json file from GCS to Bigquery. Schema-on-read is imposed.
    load_to_temp_bq = GCSToBigQueryOperator(
        task_id="load_to_temp_bq",
        write_disposition="WRITE_TRUNCATE",
        gcp_conn_id="google_cloud_default",
        bucket=GCS_BUCKET,
        source_objects=["{{ ti.xcom_pull('api_to_gcs') }}"],
        destination_project_dataset_table=TEMP_TABLE,
        source_format="NEWLINE_DELIMITED_JSON",
        schema_fields=json.load(open("{}/schema/temp_table.json".format(dag_folder))),
        ignore_unknown_values=True,
    )

    # Update the final table with the temp table, temp table can be deleted later
    temp_to_final_table = BigQueryInsertJobOperator(
        task_id="temp_to_final_table",
        project_id=GCP_PROJECT_ID,
        configuration={
            "query": {
                "query": "{% include 'sql/create_and_merge.sql' %}",
                "useLegacySql": False,
                "priority": "BATCH",
            }
        },
        params={
            "target_table": FINAL_TABLE,
            "source_table": TEMP_TABLE,
        },
    )

    latest_only >> api_to_gcs >> load_to_temp_bq >> temp_to_final_table
