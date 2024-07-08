SELECT 
    n.n_nationkey,
    n.n_name,
    n.n_regionkey,
    timez as timezone
FROM 
    {{ source('raw', 'raw_nation') }} n
JOIN 
    (SELECT c2, max(c3) timez
     FROM {{ source('mias', 'timezones') }}
     GROUP BY c2) t
ON 
    UPPER(n.N_NAME) = UPPER(t.c2)