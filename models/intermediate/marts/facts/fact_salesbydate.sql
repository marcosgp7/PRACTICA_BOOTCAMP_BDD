/*

{{ config(
    materialized='incremental',
    unique_key=['o_orderkey', 'l_suppkey', 'l_partkey']
) }}
WITH extended_prices AS (
    SELECT 
        l.l_orderkey,
        l.l_partkey,
        l.l_suppkey,
        l.l_quantity,
        ROUND((l.l_extendedprice * (1 - l.l_discount) * (1 + l.l_tax)), 2) AS total_amount,
        l.l_returnflag,
        l.l_commitdate,
        l.l_receiptdate,
        p.p_name,
        p.s_name
    FROM {{ ref('stg_lineitem') }} l
    JOIN {{ ref('dim_partsupp') }} p ON l.l_partkey = p.ps_partkey
    WHERE l.l_returnflag != 'A'
),
  adjusted_times AS (
    SELECT
        o.o_orderkey,
        o.o_orderdate,
        o.hora_UTC,
        o.o_custkey,
        o.o_clerk,
        c.c_name,
        UNIFORM(1, 48, RANDOM()) AS store_id,
        TIMESTAMPADD(HOUR, n_store.diferencia_UTC, o.hora_UTC) AS hora_local_store,
        TIMESTAMPADD(HOUR, n_cliente.diferencia_UTC, o.hora_UTC) AS hora_local_cliente,
        n_store.tipo_cambio AS cambio_store,
        n_cliente.tipo_cambio AS cambio_cliente
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('dim_customer') }} c ON o.o_custkey = c.c_custkey
    JOIN {{ ref('dim_store') }} st ON UNIFORM(1, 48, RANDOM()) = st.st_store_id
    JOIN {{ ref('dim_nationregion') }} n_store ON st.n_name = n_store.n_name
    JOIN {{ ref('dim_nationregion') }} n_cliente ON c.n_name = n_cliente.n_name
)
SELECT 
    at.o_orderkey,
    at.o_orderdate,
    at.hora_UTC,
    at.o_custkey,
    at.c_name,
    ep.l_partkey,
    ep.p_name,
    ep.l_suppkey,
    ep.s_name,
    ep.l_quantity,
    ep.total_amount,
    at.o_clerk,
    ROUND(at.cambio_store * ep.total_amount, 2) AS divisa_store,
    ROUND(at.cambio_cliente * ep.total_amount, 2) AS divisa_cliente,
    CASE 
        WHEN ep.l_returnflag = 'R' THEN 'Returned'
        WHEN ep.l_returnflag = 'N' THEN 'Not returned'
    END AS l_returnflag,
    at.hora_local_store,
    at.hora_local_cliente,
    CASE
        WHEN DATEDIFF(day, ep.l_commitdate, ep.l_receiptdate) <= 0 THEN 'En plazo, el pedido se ha entregado a tiempo o antes de la fecha estimada (COMMITDATE)'
        WHEN DATEDIFF(day, ep.l_commitdate, ep.l_receiptdate) > 10 THEN 'Fuera de plazo, el pedido se ha entregado con un retraso mayor a 10 días'
        WHEN DATEDIFF(day, ep.l_commitdate, ep.l_receiptdate) > 0 THEN 'Entrega tardía, el pedido se ha entregado como máximo 10 días después de la fecha estimada'
    END AS plazo_entrega
FROM extended_prices ep
JOIN adjusted_times at ON ep.l_orderkey = at.o_orderkey
{% if is_incremental() %}
WHERE (at.o_orderkey, ep.l_suppkey, ep.l_partkey) NOT IN (
    SELECT o_orderkey, l_suppkey, l_partkey 
    FROM {{ this }}
)
{% endif %}
ORDER BY at.o_orderdate DESC

*/

{{ config(
    materialized='incremental',
    unique_key=['o_orderkey', 'l_suppkey', 'l_partkey']
) }}

WITH extended_prices AS (
    SELECT
        ps.p_partkey AS partkey,
        s.s_suppkey AS suppkey,
        c.c_custkey AS custkey,
        e.eventkey AS eventkey,
        rn.change_type AS change_type,
        c.nation AS customer_nation
    FROM {{ ref('stg_partsupp') }} ps
    JOIN {{ ref('stg_supplier') }} s ON s.suppkey = ps.suppkey
)