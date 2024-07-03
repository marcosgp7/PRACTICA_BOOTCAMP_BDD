WITH salesbydate AS (
    SELECT
        o_orderkey, o_custkey,
        o_orderstatus, o_totalprice,
        o_orderdate, o_orderpriority,
        o_clerk, o_quantity,
        l.l_quantity AS quantity,
        ROUND(l.l_extendedprice * (1 - l.l_discount) * (1 + l.l_tax), 2) AS totalprice,
        CASE l.l_returnflag
            WHEN 'R' THEN 'Returned'
            WHEN 'N' THEN 'Not returned'
        END AS return_flag
    FROM {{ ref('stg_orders') }} o
    JOIN {{ ref('stg_lineitem') }} l ON o.o_orderkey = l.l_orderkey
    WHERE l.l_returnflag <> 'A'
)
SELECT * FROM salesbydate
ORDER BY orderdate