WITH stg_nation AS (
    SELECT *
    FROM RAW_PROBOOTCAMP.RAW.RAW_NATION
)
SELECT *
FROM {{ source('PBT_MA', 'nation') }}