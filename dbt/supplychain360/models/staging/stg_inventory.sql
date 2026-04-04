WITH raw_inventory AS ( SELECT *
    FROM {{ source('supplychain360', 'inventory') }}
)
SELECT
    DATA:"warehouse_id"::STRING        AS warehouse_id,
    DATA:"product_id"::STRING          AS product_id,
    DATA:"quantity_available"::NUMBER  AS quantity_available,
    DATA:"reorder_threshold"::NUMBER   AS reorder_threshold,
    DATA:"snapshot_date"::DATE         AS snapshot_date,
    DATA:"ingestion_timestamp"::timestamp_ntz AS ingestion_timestamp
FROM raw_inventory
