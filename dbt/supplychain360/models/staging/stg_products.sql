WITH raw_products AS ( SELECT *
    FROM {{ source('supplychain360', 'products') }}
)

SELECT
    DATA:"product_id"::STRING      AS product_id,
    DATA:"product_name"::STRING    AS product_name,
    DATA:"brand"::STRING           AS brand,
    DATA:"category"::STRING        AS category,
    DATA:"supplier_id"::STRING     AS supplier_id,
    DATA:"unit_price"::FLOAT       AS unit_price,
    DATA:"ingestion_timestamp"::timestamp_ntz AS ingestion_timestamp
FROM raw_products
