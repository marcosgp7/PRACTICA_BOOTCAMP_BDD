WITH stg_lineitem AS (
    SELECT *
    FROM RAW_PROBOOTCAMP.RAW.RAW_LINEITEM
)
SELECT *
FROM {{ source('PBT_MA', 'lineitem') }}