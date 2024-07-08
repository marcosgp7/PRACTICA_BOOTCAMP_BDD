
SELECT 
    store_id,
    sum(l_quantity) as cantidad,
    sum(total_amount) as total,
    sum(divisa_store) as total_divisa_store
from {{ ref('fact_salesbydate') }} 
group by store_id

