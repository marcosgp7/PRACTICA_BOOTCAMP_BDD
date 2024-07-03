WITH stg_customer AS (
    SELECT *
    FROM RAW_PROBOOTCAMP.RAW.RAW_CUSTOMER
)
SELECT *
FROM {{ source('PBT_MA', 'customers') }}