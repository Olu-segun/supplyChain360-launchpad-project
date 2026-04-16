{{ config(
    materialized='incremental',
    unique_key='shipment_id',
    incremental_strategy='merge'
) }}

WITH raw_shipments AS (

    SELECT *
    FROM {{ source('supplychain360', 'shipments') }}

    {% if is_incremental() %}
        WHERE DATA:"ingestion_timestamp"::timestamp_ntz >= (
            SELECT COALESCE(MAX(ingestion_timestamp), TO_TIMESTAMP('1900-01-01')) - INTERVAL '1 DAY'
            FROM {{ this }}
        )
    {% endif %}

)

SELECT
    DATA:"shipment_id"::STRING              AS shipment_id,
    DATA:"shipment_date"::DATE              AS shipment_date,
    DATA:"expected_delivery_date"::DATE     AS expected_delivery_date,
    DATA:"actual_delivery_date"::DATE       AS actual_delivery_date,
    DATA:"carrier"::STRING                  AS carrier,
    DATA:"product_id"::STRING               AS product_id,
    DATA:"store_id"::STRING                 AS store_id,
    DATA:"warehouse_id"::STRING             AS warehouse_id,
    DATA:"quantity_shipped"::NUMBER         AS quantity_shipped,
    DATA:"ingestion_timestamp"::timestamp_ntz AS ingestion_timestamp
FROM raw_shipments