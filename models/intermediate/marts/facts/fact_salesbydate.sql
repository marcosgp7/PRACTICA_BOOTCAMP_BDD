
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
        name_part,
        NAME_SUPPLIER
    FROM {{ ref('stg_lineitem') }} l
    JOIN {{ ref('dim_partsupp') }} p ON l.l_partkey = p.partkey
    WHERE l.l_returnflag != 'A'
),
adjusted_times AS (
    SELECT
        o.o_orderkey,
        o.o_orderdate,
        o.hora_UTC,
        o.o_custkey,
        o.o_clerk,
        c.name,
        n_cliente.nation as nationCLIENTE,
        UNIFORM(1, 48, RANDOM()) AS store_id,
        n_store.NATION NATIONSTORE,
        n_store.CHANGE_TYPE AS cambio_store,
        n_cliente.CHANGE_TYPE AS cambio_cliente,
    CONVERT_TIMEZONE('UTC' , n_store.timezone , o.hora_UTC ) AS hora_local_store,
    CONVERT_TIMEZONE('UTC' , n_cliente.timezone , o.hora_UTC ) AS hora_local_cliente,
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('dim_customers') }} c ON o.o_custkey = c.CUSTKEY
    JOIN {{ ref('dim_store') }} st ON UNIFORM(1, 48, RANDOM()) = st.STOREKEY
    JOIN {{ ref('dim_reg_nation') }} n_store ON st.NATION = n_store.NATION
    JOIN {{ ref('dim_reg_nation') }} n_cliente ON c.NATION = n_cliente.NATION
)
SELECT 
    eventkey,
    at.o_orderkey,
    at.o_orderdate,
    at.hora_UTC,
    at.o_custkey,
    at.name,
    store_id,
    ep.l_partkey,
    ep.name_part,
    ep.l_suppkey,
    ep.NAME_SUPPLIER,
    ep.l_quantity,
    ep.total_amount,
    at.o_clerk,
    at.cambio_store,
    at.cambio_cliente,
    at.nationCLIENTE as nationCLIENTE,
    AT.NATIONSTORE AS NATIONSTORE,
    ROUND(at.cambio_store * ep.total_amount, 2) AS divisa_store,
    ROUND(at.cambio_cliente * ep.total_amount, 2) AS divisa_cliente,
    at.hora_local_store,
    at.hora_local_cliente,
    CASE 
        WHEN ep.l_returnflag = 'R' THEN 'Returned'
        WHEN ep.l_returnflag = 'N' THEN 'Not returned'
    END AS l_returnflag,
    CASE
        WHEN DATEDIFF(day, ep.l_commitdate, ep.l_receiptdate) <= 0 THEN 'En plazo, el pedido se ha entregado a tiempo o antes de la fecha estimada (COMMITDATE)'
        WHEN DATEDIFF(day, ep.l_commitdate, ep.l_receiptdate) > 10 THEN 'Fuera de plazo, el pedido se ha entregado con un retraso mayor a 10 días'
        WHEN DATEDIFF(day, ep.l_commitdate, ep.l_receiptdate) > 0 THEN 'Entrega tardía, el pedido se ha entregado como máximo 10 días después de la fecha estimada'
    END AS plazo_entrega
FROM extended_prices ep
JOIN adjusted_times at ON ep.l_orderkey = at.o_orderkey
left join {{ source('mias', 'events') }} on upper(nation_name)=upper(nationstore) and o_orderdate between DATE_BEGIN and DATE_END 
   {% if is_incremental() %}
        where o_orderkey not in (select o_orderkey from {{ this }})
    {% endif %}
ORDER BY o_orderdate 