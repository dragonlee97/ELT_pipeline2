import requests
import logging
from google.cloud import storage
from google.cloud.storage.fileio import BlobWriter
from requests.adapters import HTTPAdapter

API_MAX_RETRIES = 5
TIMEOUT = 10
LIMIT = 100
BASE_URL = "https://public.opendatasoft.com/api/explore/v2.1/catalog/datasets/air-bnb-listings/exports"
# Records endpoint is subject to a limited number of returned records, while exports endpoint has no limitations:
# https://public.opendatasoft.com/api/explore/v2.1/console


def extract_from_api_upload_to_gcs(gcs_bucket, ds, ds_nodash, **kwargs):
    """
    :param gcs_bucket: The bucket where the raw data extracted from the API will be stored
    :param ds_nodash: Airflow template variable. Since he dag runs daily, this is used to name the file
    :param kwargs:
    :return: The json file name in the bucket to be passed to the next task xcom
    """
    file_path = f"{ds_nodash}/airbnb_listings.json"
    bucket = storage.Client().get_bucket(gcs_bucket)
    blob = bucket.blob(file_path)
    blob_writer = BlobWriter(blob)
    EXPORT_QUERY = "/jsonl?where=updated_date%3Ddate%27" + ds + "%27&limit=-1&refine=column_19%3A%22France%22&timezone=UTC&use_labels=false&epsg=4326"
    try:
        session = requests.Session()
        session.mount(BASE_URL, HTTPAdapter(max_retries=API_MAX_RETRIES))
        with session.get(BASE_URL + EXPORT_QUERY, timeout=TIMEOUT, stream=True) as response:
            response.raise_for_status()
            logging.info("Successfully got response from API.")
            # Avoid reading the content at once into memory for large responses.
            for chunk in response.iter_lines(chunk_size=256*1000, decode_unicode=True, delimiter="\n"):
                blob_writer.write.write(chunk+"\n")
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


if __name__ == "__main__":
    extract_from_api_upload_to_gcs("a", "b")