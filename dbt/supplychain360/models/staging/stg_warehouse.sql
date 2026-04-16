{{ config(
    materialized='incremental',
    unique_key='warehouse_id',
    incremental_strategy='merge'
) }}

WITH raw_warehouses AS (

    SELECT *
    FROM {{ source('supplychain360', 'warehouses') }}

    {% if is_incremental() %}
        WHERE DATA:"ingestion_timestamp"::timestamp_ntz >= (
            SELECT COALESCE(MAX(ingestion_timestamp), TO_TIMESTAMP('1900-01-01')) - INTERVAL '1 DAY'
            FROM {{ this }}
        )
    {% endif %}

)
SELECT
    DATA:"warehouse_id"::STRING        AS warehouse_id,
    DATA:"city"::STRING                AS city,
    DATA:"state"::STRING               AS state,
    DATA:"ingestion_timestamp"::timestamp_ntz AS ingestion_timestamp
FROM raw_warehouses