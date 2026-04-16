{{ config(materialized='incremental') }}

WITH raw_inventory AS (

    SELECT *
    FROM {{ source('supplychain360', 'inventory') }}

    {% if is_incremental() %}
        WHERE DATA:"ingestion_timestamp"::timestamp_ntz > (
            SELECT COALESCE(MAX(ingestion_timestamp), TO_TIMESTAMP('1900-01-01'))
            FROM {{ this }}
        )
    {% endif %}

)
SELECT
    DATA:"warehouse_id"::STRING         AS warehouse_id,
    DATA:"product_id"::STRING           AS product_id,
    DATA:"quantity_available"::NUMBER   AS quantity_available,
    DATA:"reorder_threshold"::NUMBER    AS reorder_threshold,
    DATA:"snapshot_date"::DATE          AS snapshot_date,
    DATA:"ingestion_timestamp"::timestamp_ntz AS ingestion_timestamp
FROM raw_inventory