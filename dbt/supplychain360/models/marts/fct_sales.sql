
select 
    product_id,
    sum(sales_amount) as total_sales
from {{ ref('stg_sales_transaction') }}
group by product_id