SELECT *
FROM {{ source('raw', 'raw_customers') }}