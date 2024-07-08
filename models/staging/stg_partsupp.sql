SELECT 
    ps_partkey,
    ps_suppkey,
    ps_availqty,
    ps_supplycost
FROM {{ source('raw', 'raw_partsupp') }}