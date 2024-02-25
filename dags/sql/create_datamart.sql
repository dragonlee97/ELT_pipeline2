CREATE SCHEMA IF NOT EXISTS {{ params.dataset_id}};

CREATE TABLE IF NOT EXISTS {{ params.destination_table_id }}(
        city STRING,
        nb_listings INT64,
        nb_customers INT64
    )
PARTITION BY
    _PARTITIONDATE
CLUSTER BY
    city;