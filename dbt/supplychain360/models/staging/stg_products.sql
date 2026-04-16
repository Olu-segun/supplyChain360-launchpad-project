{{ config(
    materialized='incremental',
    unique_key='product_id'
) }}

WITH raw_products AS (

    SELECT *
    FROM {{ source('supplychain360', 'products') }}

    {% if is_incremental() %}
        WHERE DATA:"ingestion_timestamp"::timestamp_ntz > (
            SELECT COALESCE(MAX(ingestion_timestamp), TO_TIMESTAMP('1900-01-01'))
            FROM {{ this }}
        )
    {% endif %}

)

SELECT
    DATA:"product_id"::STRING       AS product_id,
    DATA:"product_name"::STRING     AS product_name,
    DATA:"brand"::STRING            AS brand,
    DATA:"category"::STRING         AS category,
    DATA:"supplier_id"::STRING      AS supplier_id,
    DATA:"unit_price"::FLOAT        AS unit_price,
    DATA:"ingestion_timestamp"::timestamp_ntz AS ingestion_timestamp
FROM raw_products