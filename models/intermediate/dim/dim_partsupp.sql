{{ config(
    materialized='incremental'
) }}
WITH dim_partsupp AS (
    SELECT
    ps_partkey AS partkey,
    ps_availqty AS quantity,
    ps_supplycost AS supplycost,
    p_name AS name_part,
    p_brand AS brand,
    p_size AS part_size,
    p_retailprice AS retailprice,
    s_name AS name_supplier,
    s_address AS supplier_address,
    n_name AS nation,
    r_name AS region
FROM {{ ref('stg_partsupp') }} ps
JOIN {{ ref('stg_part') }} p ON p.p_partkey = ps.ps_partkey
JOIN {{ ref('stg_supplier') }} s ON s.s_suppkey = ps.ps_suppkey
JOIN {{ ref('stg_nation') }} n ON n.n_nationkey = s.s_nationkey
JOIN {{ ref('stg_region') }} r ON r.r_regionkey = n.n_regionkey
) 
    SELECT * FROM dim_partsupp