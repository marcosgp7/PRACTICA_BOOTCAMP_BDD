
SELECT 
    o_custkey,
    name,
    sum(l_quantity) as cantidad_arts,
    sum(total_amount) as total,
    sum(divisa_cliente) as total_divisa_cliente
from {{ ref('fact_salesbydate') }} 

group by o_custkey,name

    


