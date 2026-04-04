WITH raw_store_locations AS ( SELECT *
    FROM {{ source('supplychain360', 'store_locations') }}
)
SELECT
    DATA:"store_id"::STRING AS store_id,
    DATA:"store_name"::STRING AS store_name,
    DATA:"city"::STRING AS city,
    DATA:"state"::STRING AS state,
    DATA:"region"::STRING AS region,
    TO_DATE(TO_TIMESTAMP(DATA:"store_open_date"::NUMBER / 1e9)) AS store_open_date,
    DATA:"ingestion_timestamp"::timestamp_ntz AS ingestion_timestamp
FROM raw_store_locations






















