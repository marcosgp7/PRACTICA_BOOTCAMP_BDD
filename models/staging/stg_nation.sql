WITH stg_nation AS (
    SELECT DISTINCT
        n_nationkey,
        n_name,
        CASE
            WHEN n_name = 'ALGERIA' THEN 1  -- Central European Time (CET) / Central European Summer Time (CEST)
            WHEN n_name = 'ARGENTINA' THEN -3  -- Argentina Time (ART)
            WHEN n_name = 'BRAZIL' THEN -3  -- Brasília Time (BRT) / Brasília Summer Time (BRST)
            WHEN n_name = 'CANADA' THEN 
                CASE
                    WHEN n_regionkey IN (1, 2, 3) THEN -5  -- Eastern Standard Time (EST) / Eastern Daylight Time (EDT)
                    WHEN n_regionkey IN (4, 5) THEN -6  -- Central Standard Time (CST) / Central Daylight Time (CDT)
                    WHEN n_regionkey = 6 THEN -7  -- Mountain Standard Time (MST) / Mountain Daylight Time (MDT)
                    WHEN n_regionkey = 7 THEN -8  -- Pacific Standard Time (PST) / Pacific Daylight Time (PDT)
                    ELSE 0  -- Otras regiones de Canadá
                END
            WHEN n_name = 'EGYPT' THEN 2  -- Eastern European Time (EET) / Eastern European Summer Time (EEST)
            WHEN n_name = 'ETHIOPIA' THEN 3  -- East Africa Time (EAT)
            WHEN n_name = 'FRANCE' THEN 1  -- Central European Time (CET) / Central European Summer Time (CEST)
            WHEN n_name = 'GERMANY' THEN 1  -- Central European Time (CET) / Central European Summer Time (CEST)
            WHEN n_name = 'INDIA' THEN 5.5  -- Indian Standard Time (IST)
            WHEN n_name = 'INDONESIA' THEN 7  -- Western Indonesia Time (WIB)
            WHEN n_name = 'IRAN' THEN 3.5  -- Iran Standard Time (IRST) / Iran Daylight Time (IRDT)
            WHEN n_name = 'IRAQ' THEN 3  -- Arabian Standard Time (AST)
            WHEN n_name = 'JAPAN' THEN 9  -- Japan Standard Time (JST)
            WHEN n_name = 'JORDAN' THEN 3  -- Arabian Standard Time (AST)
            WHEN n_name = 'KENYA' THEN 3  -- East Africa Time (EAT)
            WHEN n_name = 'MOROCCO' THEN 0  -- Western European Time (WET) / Western European Summer Time (WEST)
            WHEN n_name = 'MOZAMBIQUE' THEN 2  -- Central Africa Time (CAT)
            WHEN n_name = 'PERU' THEN -5  -- Peru Time (PET)
            WHEN n_name = 'CHINA' THEN 8  -- China Standard Time (CST)
            WHEN n_name = 'ROMANIA' THEN 2  -- Eastern European Time (EET) / Eastern European Summer Time (EEST)
            WHEN n_name = 'SAUDI ARABIA' THEN 3  -- Arabian Standard Time (AST)
            WHEN n_name = 'VIETNAM' THEN 7  -- Indochina Time (ICT)
            WHEN n_name = 'RUSSIA' THEN
                CASE
                    WHEN n_regionkey IN (1, 2, 3, 4, 5) THEN 3  -- Moscow Standard Time (MSK) / Moscow Daylight Time (MSD)
                    WHEN n_regionkey IN (6, 7, 8, 9) THEN 4  -- Yekaterinburg Time (YEKT) / Yekaterinburg Summer Time (YEKST)
                    ELSE 0  -- Otras regiones de Rusia
                END
            WHEN n_name = 'UNITED KINGDOM' THEN 0  -- Western European Time (WET) / British Summer Time (BST)
            WHEN n_name = 'UNITED STATES' THEN
                CASE
                    WHEN n_regionkey IN (1, 2, 3, 4, 5) THEN -5  -- Eastern Standard Time (EST) / Eastern Daylight Time (EDT)
                    WHEN n_regionkey IN (6, 7, 8) THEN -6  -- Central Standard Time (CST) / Central Daylight Time (CDT)
                    WHEN n_regionkey = 9 THEN -7  -- Mountain Standard Time (MST) / Mountain Daylight Time (MDT)
                    WHEN n_regionkey = 10 THEN -8  -- Pacific Standard Time (PST) / Pacific Daylight Time (PDT)
                    ELSE 0  -- Otras regiones de USA
                END
            ELSE 0  -- O cualquier valor por defecto si es necesario
        END AS diferencia_utc
    FROM {{ source('raw', 'raw_nation') }} n
    JOIN {{ source('raw', 'raw_region') }} r ON n.n_regionkey = r.r_regionkey
)
SELECT *
FROM stg_nation
ORDER BY n_nationkey