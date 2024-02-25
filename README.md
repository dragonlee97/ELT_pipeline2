# Stacks
- Airflow 2.5.3 (Configured in Dockerfile)
- Python 3.9 (Configured in Dockerfile)
- GCP (GCS bukcet; Bigquery)
- Minio 

# Repo Structure
- `dags`: 
  - `pipeline.py`: DAG code
  - `helpers`: utils functions that are used in the dag
  - `schema`: json files which define the table schema
  - `sql`: templated sql files which are used in the dag
- `companies`: provided: csv files mounted to Minio
- `Dockerfile`: build image for airflow
- `docker-compose.yaml`: define containers of airflow and minio 
- `requirements.txt`: python libraries that the project depends on
- `Makefile`

# Prerequisite & Set up 
1. You should have the GCP account and your `application_default_credentials.json` (relative path is in docker-compose) saved in your local machine. Make sure you have the permissions of creating tables, buckets and jobs
2. Docker desktop should be installed on your local machine and allocate enough memory for running both airflow and minio
3. Fill up config.json file
- All the _TABLE parameters should be written in the format `project_id.dataset_id.table_id`
4. When you first start the airflow instance, you need to create a google_cloud_default connection on the airflow webserver UI. Go to Admin -> Connections. Just fill the connection
5. Execute the following `make` commands in order to run the project:
```commandline
make prepare
make run
```

# ELT 
![DAG_GRAPH](https://github.com/dragonlee97/meero_case_study/blob/main/dag.png?raw=true)
## Extract & Load
### airbnb listings
- Airflow Tasks:`airbnb_listings_to_gcs; load_airbnb_listings_to_bq`
- Flow: API -> GCS -> Bigquery raw table
- Schema change: Schema on read (Imposed schema on BQ)
- Table: `airbnb_listings` (Non-Partitioned; Truncate & Insert)

### customer companies
- Airflow Tasks: `create_companies_table_if_not_exists; companies_to_temp_bigquery; load_temp_companies_to_bq`
- Flow: Minio -> staging table -> Bigquery raw table
- Schema change: Schema on write (Imposed schema on staging table)
- Table: `companies` (Non-Partitioned; Merge/Upsert)

## Transform
### monitoring datamart 
- Airflow Tasks: `create_monitoring_datamart_if_not_exists; compute_final_datamart`
- SQL Transformation: count of listing and customers group by city and join two CTEs on Upper Case `city` field 
- Table: `monitoring` (Partitioned)

### Tables

