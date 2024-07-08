{{ config(
    materialized='incremental'
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
            WHEN DATEDIFF(day, L_COMMITDATE, L_RECEIPTDATE) > 30 THEN 0  -- Fuera de plazo (> 30 días)
            WHEN DATEDIFF(day, L_COMMITDATE, L_RECEIPTDATE) <= 0 THEN 1  -- En plazo (0 días de retraso)
            WHEN DATEDIFF(day, L_COMMITDATE, L_RECEIPTDATE) <= 10 THEN 2 -- Entrega tardía (<= 10 días de retraso)
            WHEN DATEDIFF(day, L_COMMITDATE, L_RECEIPTDATE) > 10 AND DATEDIFF(day, L_COMMITDATE, L_RECEIPTDATE) <= 30 THEN 3 -- Entrega crítica (entre 10 y 30 días de retraso)
            ELSE 0
        END AS plazo_entrega,
        current_timestamp as update_date
FROM extended_prices ep
JOIN adjusted_times at ON ep.l_orderkey = at.o_orderkey
left join {{ ref('dim_events') }} on upper(nation_name)=upper(nationstore) and o_orderdate between DATE_BEGIN and DATE_END 
   {% if is_incremental() %}
        where o_orderkey not in (select o_orderkey from {{ this }})
    {% endif %}
ORDER BY o_orderdate 