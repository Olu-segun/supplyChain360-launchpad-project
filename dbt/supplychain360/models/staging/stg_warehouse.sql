WITH raw_warehouses AS ( SELECT *
    FROM {{ source('supplychain360', 'warehouses') }}
)
SELECT
    DATA:"warehouse_id"::STRING   AS warehouse_id,
    DATA:"city"::STRING           AS city,
    DATA:"state"::STRING          AS state,
    DATA:"ingestion_timestamp"::timestamp_ntz AS ingestion_timestamp
FROM raw_warehouses
