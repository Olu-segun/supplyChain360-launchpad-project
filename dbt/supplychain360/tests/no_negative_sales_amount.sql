SELECT *
FROM {{ ref('stg_sales_transaction') }}
WHERE sales_amount  < 0