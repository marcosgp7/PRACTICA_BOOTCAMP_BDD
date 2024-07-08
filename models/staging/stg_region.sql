SELECT
    r_regionkey,
    r_name
FROM {{ source('raw', 'raw_region') }}