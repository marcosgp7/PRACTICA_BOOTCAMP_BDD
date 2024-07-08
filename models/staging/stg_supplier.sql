SELECT
    s_suppkey,
    s_name,
    s_address,
    s_nationkey,
    s_phone,
    s_acctbal
FROM {{ source('raw', 'raw_supplier') }}