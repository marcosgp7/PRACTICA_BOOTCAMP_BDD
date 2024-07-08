{{ config(
    materialized='incremental'
) }}
WITH dim_reg_nat AS (
    SELECT 
        n_nationkey AS nationkey,
        currency_name AS currency,
        n_name AS nation,
        r_name AS region,
        change_type,
        timezone
    FROM {{ ref('stg_nation') }} n
    JOIN {{ source('mias', 'currency') }} ON nationkey = n.n_nationkey
    JOIN {{ ref('stg_region') }} r ON r.r_regionkey = n.n_regionkey
)
    SELECT * FROM dim_reg_nat
    ORDER BY nation