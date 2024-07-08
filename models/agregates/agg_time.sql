select 
        month(o_orderdate) as order_month, 
        year(o_orderdate) as order_year,
        count(*) as lines,
        count(distinct O_ORDERKEY) as orders
    from {{ ref('fact_salesbydate') }}
    group by order_year, order_month