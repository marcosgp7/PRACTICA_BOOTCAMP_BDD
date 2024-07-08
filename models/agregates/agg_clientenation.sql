{{ config(
    materialized='incremental'
) }}
SELECT 
    nationCLIENTE,
    count(distinct o_custkey) as num_clientes,
    sum(l_quantity) as cantidad_arts,
    sum(total_amount) as total,
    sum(divisa_cliente) as total_divisa_cliente
from {{ ref('fact_salesbydate') }} 

group by nationCLIENTE
order by num_clientes desc

