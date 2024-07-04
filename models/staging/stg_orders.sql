    SELECT o_orderkey,
    o_custkey,
    o_orderstatus,
    o_totalprice,
    concat(o_orderdate,' ', TO_CHAR(CURRENT_TIMESTAMP, 'HH24:MI:SS')) as hora_UTC,
    o_orderpriority,
    CAST(REGEXP_REPLACE(o_clerk, '[^0-9]', '') AS NUMBER) AS o_clerk,
    /*
    En esta parte, se utiliza una expresión regular --> '[^0-9]', ''
    Lo que está entre corchetes quiere decir: 'Cualquier carácter que no
    sea un dígito' y lo que hace es eliminarlo, al ir seguido por la cadena
    de reemplazo --> ''
    */
FROM {{ source('raw', 'raw_orders') }}