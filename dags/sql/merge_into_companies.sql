MERGE {{ params.customers_table }} AS Target
USING {{ params.bigquery_temp_table }}	AS Source
ON Source.id = Target.id

-- Updates
WHEN MATCHED THEN
    UPDATE SET
    Target.id = SAFE_CAST(Source.id AS INT64),
    Target.address = Source.address,
    Target.city = Source.city,
    Target.zip = SAFE_CAST(Source.zip AS STRING),
    Target.updated_at = Source.created_at

-- Inserts
WHEN NOT MATCHED BY Target THEN
    INSERT (id, address, city, zip, created_at)
    VALUES (id, address, city, zip, created_at)
