WITH stg_supplier AS (
    SELECT *
    FROM RAW_PROBOOTCAMP.RAW.RAW_SUPPLIER
)
    SELECT *
FROM {{ source('PBT_MA', 'supplier') }}