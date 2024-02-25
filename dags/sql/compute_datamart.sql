-- The City fields are forced to uppercase in order to be join on
-- Many rows in Customers table are missing city field, which can be enriched by Zip Code
WITH stg_listings AS (
    SELECT UPPER(city) AS city, COUNT(DISTINCT id) AS nb_listings
    FROM {{ params.listings_table }}
    GROUP BY city
),

stg_customers AS (
    SELECT UPPER(city) AS city, COUNT(DISTINCT id) AS nb_customers
    FROM {{ params.customers_table }}
    GROUP BY city
)

SELECT COALESCE(stg_listings.city, stg_customers.city) AS city, nb_listings, nb_customers
FROM stg_listings
FULL JOIN stg_customers
ON stg_listings.city = stg_customers.city