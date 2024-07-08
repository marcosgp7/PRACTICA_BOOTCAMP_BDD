{{ config(
    materialized='incremental'
) }}
SELECT 
    o_custkey,
    name,
    sum(l_quantity) as cantidad_arts,
    sum(total_amount) as total,
    sum(divisa_cliente) as total_divisa_cliente
from {{ ref('fact_salesbydate') }} 
{% if is_incremental() %}
        where o_custkey not in (select o_custkey from {{ this }})
    {% endif %}
group by o_custkey,name


