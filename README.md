# Stacks
- Airflow 2.5.3 (Configured in Dockerfile)
- Python 3.9 (Configured in Dockerfile)
- GCP (GCS bukcet; Bigquery)
- Minio 


# Prerequisite
1. You should have the GCP account and your application_default_credentials.json saved in your local machine. Make sure you have the permissions of creating tables, buckets
2. Docker desktop should be installed on your local machine and allocate enough memory for running both airflow and minio

# Set up & Running
3. When you first start the airflow instance, you need to set up a google_cloud_default connection on the airflow webserver UI. Admin -> Connections

# ELT tasks
# Extract & Load
**airbnb listing tasks**: airbnb_listings_to_gcs; load_airbnb_listings_to_bq
**minio customer companies tasks**: create_companies_table_if_not_exists; companies_to_temp_bigquery; load_temp_companies_to_bq
## Transform
**monitoring datamart tasks**: