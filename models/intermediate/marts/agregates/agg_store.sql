SELECT 
    store_id,
    sum(l_quantity),
    sum(total_amount),
    sum(divisa_store)
from {{ ref('fact_salesbydate') }} 
group by store_id

