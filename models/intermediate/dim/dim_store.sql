WITH dim_store AS (
    SELECT 
    storekey,
    nationkey,
    n_name AS nation,
    r_name AS region
FROM {{ source('mias', 'store') }} st
JOIN {{ ref('stg_nation') }} n ON st.nationkey = n.n_nationkey
JOIN {{ ref('stg_region') }} r ON r.r_regionkey = n.n_regionkey
)
    SELECT * FROM dim_store