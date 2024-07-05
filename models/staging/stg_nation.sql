
SELECT 
    n.*, 
    t.c3 as timezone
FROM 
    {{ source('raw', 'raw_nation') }} n
JOIN 
    (SELECT c2, c3 
     FROM {{ source('mias', 'timezones') }}
     GROUP BY c2, c3) t
ON 
    UPPER(n.N_NAME) = UPPER(t.c2)

