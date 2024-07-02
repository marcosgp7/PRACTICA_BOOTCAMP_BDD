    SELECT
    o_orderkey AS orderkey,
    o_orderstatus AS status,
    l_extendedprice * (1 - l_discount) * (1 + l_tax) AS totalprice,
    o_orderdate AS orderdate,
    o_clerk AS clerk,
    CASE l_returnflag
        WHEN 'R' THEN 'Returned'
        WHEN 'N' THEN 'Not returned'
    END AS return_flag
    FROM RAW_PROBOOTCAMP.RAW.RAW_ORDERS
    JOIN RAW_PROBOOTCAMP.RAW.RAW_LINEITEM ON o_orderkey = l_orderkey
    ORDER BY orderdate