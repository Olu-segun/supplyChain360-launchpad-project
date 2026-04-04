WITH raw_suppliers AS ( SELECT *
    FROM {{ source('supplychain360', 'suppliers') }}
)
SELECT
    DATA:"supplier_id"::STRING     AS supplier_id,
    DATA:"supplier_name"::STRING   AS supplier_name,
    DATA:"category"::STRING        AS category,
    DATA:"country"::STRING         AS country,
    DATA:"ingestion_timestamp"::timestamp_ntz AS ingestion_timestamp
FROM raw_suppliers
