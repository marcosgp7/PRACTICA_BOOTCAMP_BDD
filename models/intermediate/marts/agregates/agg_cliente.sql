SELECT 
    o_custkey,
    name,
    sum(l_quantity),
    sum(total_amount),
    sum(divisa_cliente)
from {{ ref('fact_salesbydate') }} 
group by o_custkey,name

