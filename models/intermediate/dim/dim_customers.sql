{{ config(
    materialized='incremental'
) }}
WITH dim_customer AS (
    SELECT 
        c.c_custkey AS custkey,
        c.c_name AS name,
        c.c_address AS address,
        c.c_phone AS phone,
        c.c_acctbal AS acctbal,
        c.c_mktsegment AS mktsegment,
        n.n_name AS nation,
        r.r_name AS region,
        cr.change_type AS currency
    FROM {{ ref('stg_customers') }} c
    JOIN {{ source('mias', 'currency') }} cr ON cr.nationkey = c.c_nationkey
    JOIN {{ ref('stg_nation') }} n ON n.n_nationkey = c.c_nationkey
    JOIN {{ ref('stg_region') }} r ON r.r_regionkey = n.n_regionkey
)
SELECT * FROM dim_customer