    SELECT *
FROM {{ source('raw', 'raw_partsupp') }}