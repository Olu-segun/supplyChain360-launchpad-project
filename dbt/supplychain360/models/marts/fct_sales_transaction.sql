SELECT
   store_id,
   product_id,
   sum(sales_amount) as total_sales,
   ingestion_timestamp
FROM {{ ref('stg_sales_transaction') }}
GROUP BY product_id, store_id, ingestion_timestamp