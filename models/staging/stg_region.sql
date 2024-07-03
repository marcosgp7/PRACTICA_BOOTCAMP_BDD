WITH stg_region AS (
    SELECT *
    FROM RAW_PROBOOTCAMP.RAW.RAW_REGION
)
    SELECT *
FROM {{ source('PBT_MA', 'region') }}