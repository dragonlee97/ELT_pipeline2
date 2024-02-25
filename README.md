# Stacks
- Airflow 2.5.3 (Configured in Dockerfile)
- Python 3.9 (Configured in Dockerfile)
- GCP (GCS bucket; Bigquery)
- Minio 

<br>

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
- `Makefile`: simplified procedure commands to build and run airflow and minio

<br>

# Prerequisites & Set up 
1. You should have the GCP account and your `application_default_credentials.json` (relative path is in docker-compose) saved in your local machine. Make sure you have the permissions of creating tables, buckets and jobs
2. Docker desktop should be installed on your local machine and allocate enough memory for running both airflow and minio
3. Fill `config.json` file with your own configs: All the `_TABLE` parameters should align with the format `project_id.dataset_id.table_id`
4. Execute the following `make` commands in order to start airflow and minio:
```commandline
make prepare
make run
```
5. Go to Airflow UI at `localhost:8080` and login with `username=airflow` and `password=airflow`
6. When you first start the airflow instance, you need to create a `google_cloud_default` connection on the airflow webserver UI. Go to Admin -> Connections. Just fill the Connection Id and Type.
- Connection id: google_cloud_default
- Connection Type: Google Cloud

<br>

# ELT 
![DAG_GRAPH](https://github.com/dragonlee97/meero_case_study/blob/main/dag.png?raw=true)
## Extract & Load
### Airbnb Listings
- Airflow Tasks:`airbnb_listings_to_gcs; load_airbnb_listings_to_bq`
- Flow: API -> GCS -> Bigquery raw table
- Schema change: Schema on read (Imposed schema on BQ)
- Table: `airbnb_listings` (Non-Partitioned; Truncate & Insert)

| Column        | Type    | Description                                  |
|---------------|---------|----------------------------------------------|
| id            | integer | Airbnb's unique identifier for the listing   |                                                              |
| name          | string  | Name of the listing                          |
| host_id       | integer | Airbnb's unique identifier for the host/user |
| neighbourhood | string  | The neighbourhood name of the listing        |
| ...           | ...     | ...                                          |
| column_20     | string  |                                              |
 
The whole table and data dictionary: [See Here](https://docs.google.com/spreadsheets/d/1iWCNJcSutYqpULSQHlNyGInUvHg2BoUGoNRIGa6Szc4/edit#gid=1322284596)

<br>

### Customer Companies
- Airflow Tasks: `create_companies_table_if_not_exists; companies_to_temp_bigquery; load_temp_companies_to_bq`
- Flow: Minio -> staging table -> Bigquery raw table
  - If there is no such a csv file for today, skip the last task of updating the raw table
- Schema change: Schema on write (Imposed schema on staging table)
- Table: `companies` (Non-Partitioned; Merge/Upsert to avoid duplicates)

| Column     | Type    | Description                                                        |
|------------|---------|--------------------------------------------------------------------|
| id         | integer | Id of the company                                                  |
| address    | string  | Address of the company                                             |
| city       | string  | City where the company locates in                                  |
| zip        | string  | Zip code of the company address                                    |
| created_at | date    | The date when the company was first created                        |
| updated_at | date    | The last date when the company info was updated through a csv file |

<br>

## Transform
### Monitoring Datamart 
- Airflow Tasks: `create_monitoring_datamart_if_not_exists; compute_final_datamart`
- SQL Transformation: count of listing and customers group by city and join two CTEs on Uppercase `city` field 
- Table: `monitoring` (Daily Partitioned)

| Column         | Type    | Description                                                |
|----------------|---------|------------------------------------------------------------|
| _PARTITIONTIME | date    | Logical date of ingestion time                             |
| city           | string  | City name                                                  |
| nb_listings    | integer | Number of airbnb listing in this city                      |
| nb_customers   | integer | Number of our customers (concierge companies) in this city |

<br>

# Future Improvements
- Data enrichment with third party reference for geographical data: [geo.api.gouv.fr](https://adresse.data.gouv.fr/api-doc/adresse)
- Use airflow taskgroup for neat dag structure
- dbt integratino for sql results materialization, model reference and test
- Slack integration to send alerting message (pipeline finished; quality etc.)