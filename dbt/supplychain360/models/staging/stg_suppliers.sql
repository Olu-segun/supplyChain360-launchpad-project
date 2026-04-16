{{ config(
    materialized='incremental',
    unique_key='supplier_id',
    incremental_strategy='merge'
) }}

WITH raw_suppliers AS (

    SELECT *
    FROM {{ source('supplychain360', 'suppliers') }}

    {% if is_incremental() %}
        WHERE DATA:"ingestion_timestamp"::timestamp_ntz >= (
            SELECT COALESCE(MAX(ingestion_timestamp), TO_TIMESTAMP('1900-01-01')) - INTERVAL '1 DAY'
            FROM {{ this }}
        )
    {% endif %}

)

SELECT
    DATA:"supplier_id"::STRING        AS supplier_id,
    DATA:"supplier_name"::STRING      AS supplier_name,
    DATA:"category"::STRING           AS category,
    DATA:"country"::STRING            AS country,
    DATA:"ingestion_timestamp"::timestamp_ntz AS ingestion_timestamp
FROM raw_suppliers