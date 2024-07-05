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
        ps.name_part,
        ps.name_supplier
    FROM {{ ref('stg_lineitem') }} l
    JOIN {{ ref('dim_partsupp') }} ps ON l.l_partkey = ps.partkey
    WHERE l.l_returnflag != 'A'
), 