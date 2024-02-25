WITH stg_listings AS (
    SELECT UPPER(city) as city, count(distinct id) as nb_listings
    FROM {{ params.listings_table }}
    GROUP BY city
),

stg_customers AS (
    SELECT UPPER(city) as city, count(distinct id) as nb_customers
    FROM {{ params.customers_table }}
    GROUP BY city
)

SELECT COALESCE(stg_listings.city, stg_customers.city) as city, nb_listings, nb_customers
FROM stg_listings
FULL JOIN stg_customers
ON stg_listings.city = stg_customers.city