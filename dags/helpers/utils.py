import os
import requests
import logging
import urllib3
import pandas as pd
import minio
from minio import Minio
from google.cloud import storage
from google.cloud import bigquery
from google.cloud.storage.fileio import BlobWriter
from requests.adapters import HTTPAdapter

API_MAX_RETRIES = 5
TIMEOUT = 120

# Records endpoint is subject to a limited number of returned records, while exports endpoint has no limitations:
# https://public.opendatasoft.com/api/explore/v2.1/console
BASE_URL = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/air-bnb-listings/exports"
EXPORT_QUERY = "/jsonl?limit=-1&refine=column_19%3A%22France%22&timezone=UTC&use_labels=false&epsg=4326"


def extract_from_api_upload_to_gcs(gcs_bucket, ds_nodash, **kwargs):
    """
    Query the export endpoint and write France airbnb listings in the specified gcs bucket in NDJSON format
    :param gcs_bucket: The bucket where the raw data extracted from the API will be stored
    :param ds_nodash: Airflow template variable. Since the dag runs daily, this is used to name the file path
    :param kwargs: Other airflow template variables
    :return: The json file path in the bucket to be passed to the next task xcom
    """
    file_path = f"{ds_nodash}/airbnb_listings.json"
    bucket = storage.Client().get_bucket(gcs_bucket)
    blob = bucket.blob(file_path)
    blob_writer = BlobWriter(blob)

    try:
        session = requests.Session()
        session.mount(BASE_URL, HTTPAdapter(max_retries=API_MAX_RETRIES))
        with session.get(BASE_URL + EXPORT_QUERY, timeout=TIMEOUT, stream=True) as response:
            response.raise_for_status()
            logging.info("Successfully got response from API.")
            # Avoid reading the content at once into memory for large responses.
            for chunk in response.iter_content(chunk_size=256 * 1000):
                blob_writer.write(chunk)
    except requests.exceptions.Timeout as e:
        logging.error(f"Timeout Error after {str(API_MAX_RETRIES * TIMEOUT)} seconds: {e}")
        raise Exception
    except requests.exceptions.HTTPError as e:
        logging.error("HTTP Error: %s", e)
        raise Exception
    except requests.exceptions.RequestException as e:
        logging.error("RequestException: %s", e)
        raise Exception

    blob_writer.close()
    return file_path


def read_from_minio_load_to_temp_table(minio_bucket_name, bigquery_raw_table, ds, ds_nodash, **kwargs):
    """
    Read current logical date csv from Minio with data validation and load to bigquery raw table
    :param minio_bucket_name: minio bucket name where the customer csv files are stored
    :param bigquery_raw_table: bigquery table name where to load the customer data
    :param ds: Airflow template variable: logical date as the format of YYYY-MM-DD. This is used to identify the csv file in the minio bucket
    :param ds_nodash: Airflow template variable: logical date as the format of YYYYMMDD. This is used to specify the partition of bq table
    :param kwargs: Other airflow template variables
    """
    # Minio is accessible through the network of its service name in docker compose
    minio_client = Minio('minio:9000',
                         access_key=os.getenv("MINIO_ROOT_USER"),
                         secret_key=os.getenv("MINIO_ROOT_PASSWORD"),
                         secure=False
                         )

    # Check if the MINIO_BUCKET_NAME exists.
    if not minio_client.bucket_exists(minio_bucket_name):
        logging.error("%s not found", minio_bucket_name)
        raise Exception

    try:
        response = minio_client.get_object(minio_bucket_name, str(ds)+".csv")
        df = pd.read_csv(response, sep=",", header=0, converters={"zip": str})
    except minio.error.S3Error as e:
        if e.code == 'NoSuchKey':
            # Successfully quit the task if there is no csv file for this logical date
            logging.info('No object found - returning empty')
            return
    except urllib3.exceptions.HTTPError as e:
        logging.error('Request failed: %s', e.reason)
        raise Exception

    response.close()
    response.release_conn()

    # data validation before loading to bigquery: id should be unique and created_at should equal to execution_date
    if not df["id"].is_unique:
        logging.error("id column in the csv file is not unique.")
        raise ValueError

    if len(df[df["created_at"]!=str(ds)])>0:
        logging.error("created_at column contains wrong date.")
        raise ValueError

    bigquery_client = bigquery.Client()
    partitioned_destination_table = bigquery_raw_table + "$" + str(ds_nodash)
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("id", "STRING", "REQUIRED"),
            bigquery.SchemaField("address", "STRING", "NULLABLE"),
            bigquery.SchemaField("city", "STRING", "NULLABLE"),
            bigquery.SchemaField("zip", "STRING", "NULLABLE"),
            bigquery.SchemaField("created_at", "DATE", "REQUIRED"),
        ],
        field_delimiter=",",
        null_marker="",
        ignore_unknown_values=True,
        source_format=bigquery.SourceFormat.CSV,
        create_disposition="CREATE_IF_NEEDED",
        write_disposition="WRITE_TRUNCATE"
    )
    load_job = bigquery_client.load_table_from_dataframe(
        df, partitioned_destination_table, job_config=job_config
    )
    load_job.result()

