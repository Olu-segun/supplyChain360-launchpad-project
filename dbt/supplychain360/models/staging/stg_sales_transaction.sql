{{ config(
    materialized='incremental',
    unique_key='transaction_id',
    incremental_strategy='merge'
) }}

WITH raw_sales AS (

    SELECT *
    FROM {{ source('supplychain360', 'sales') }}

    {% if is_incremental() %}
        WHERE DATA:"ingestion_timestamp"::timestamp_ntz > (
            SELECT COALESCE(MAX(ingestion_timestamp), TO_TIMESTAMP('1900-01-01'))
            FROM {{ this }}
        )
    {% endif %}

)

SELECT
    DATA:"transaction_id"::STRING                   AS transaction_id,
    TO_TIMESTAMP(DATA:"transaction_timestamp"::NUMBER / 1e9) AS transaction_timestamp,
    DATA:"store_id"::STRING                         AS store_id,
    DATA:"product_id"::STRING                       AS product_id,
    DATA:"quantity_sold"::NUMBER                    AS quantity_sold,
    DATA:"unit_price"::FLOAT                        AS unit_price,
    DATA:"sale_amount"::FLOAT                       AS sales_amount,
    DATA:"discount_pct"::FLOAT                      AS discount_pct,
    DATA:"ingestion_timestamp"::timestamp_ntz       AS ingestion_timestamp
FROM raw_sales