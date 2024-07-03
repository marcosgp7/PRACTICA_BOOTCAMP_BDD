    SELECT
    o_orderkey AS orderkey,
    o_orderstatus AS status,
    l_extendedprice * (1 - l_discount) * (1 + l_tax) AS totalprice,
    o_orderdate AS orderdate,
    o_clerk AS clerk,
    CASE l_returnflag
        WHEN 'R' THEN 'Returned'
        WHEN 'N' THEN 'Not returned'
    END AS return_flag,
    c_name AS customer_name,
    p_partkey AS partkey,
    p_name AS part_name
    FROM {{ ref('stg_orders') }}
    JOIN {{ ref('stg_lineitem') }} ON o_orderkey = l_orderkey
    JOIN {{ ref('stg_customers') }} ON o_custkey = c_custkey
    JOIN {{ ref('stg_part') }} ON l_partkey = p_partkey
    ORDER BY orderdate