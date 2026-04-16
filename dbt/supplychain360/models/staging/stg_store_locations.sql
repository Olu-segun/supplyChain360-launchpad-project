{{ config(
    materialized='incremental',
    unique_key='store_id',
    incremental_strategy='merge'
) }}

WITH raw_store_locations AS (

    SELECT *
    FROM {{ source('supplychain360', 'store_locations') }}

    {% if is_incremental() %}
        WHERE DATA:"ingestion_timestamp"::timestamp_ntz >= (
            SELECT COALESCE(MAX(ingestion_timestamp), TO_TIMESTAMP('1900-01-01')) - INTERVAL '1 DAY'
            FROM {{ this }}
        )
    {% endif %}

)

SELECT
    DATA:"store_id"::STRING                     AS store_id,
    DATA:"store_name"::STRING                   AS store_name,
    DATA:"city"::STRING                         AS city,
    DATA:"state"::STRING                        AS state,
    DATA:"region"::STRING                       AS region,
    TO_DATE(TO_TIMESTAMP(DATA:"store_open_date"::NUMBER / 1e9)) AS store_open_date,
    DATA:"ingestion_timestamp"::timestamp_ntz   AS ingestion_timestamp
FROM raw_store_locations