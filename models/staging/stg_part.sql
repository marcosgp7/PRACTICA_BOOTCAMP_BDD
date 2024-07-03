WITH stg_part AS (
    SELECT *
    FROM RAW_PROBOOTCAMP.RAW.RAW_PART
)
    SELECT *
FROM {{ source('PBT_MA', 'part') }}