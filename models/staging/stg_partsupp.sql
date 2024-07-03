WITH stg_partsupp AS (
    SELECT *
    FROM RAW_PROBOOTCAMP.RAW.RAW_PARTSUPP
)
    SELECT *
FROM {{ source('PBT_MA', 'partsupp') }}