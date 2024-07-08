{{ config(
    materialized='incremental'
) }}
SELECT 
    NATIONSTORE,
    count(distinct o_custkey) as num_clientes,
    sum(l_quantity) as cantidad_arts,
    sum(total_amount) as total,
    sum(divisa_store) as total_divisa_store
from {{ ref('fact_salesbydate') }} 
group by nationstore
order by num_clientes desc