{{ config(
    materialized = 'view',
    tags = ['silver','centros'],
) }}

WITH lvl1 AS (
    SELECT DISTINCT
        centro_dep,
        centro_sup,
        1 AS nivel,
        CAST(centro_sup AS STRING) AS family
    FROM {{ ref('tr_centros1') }}
),

lvl2 AS (
    SELECT
        c.centro_dep,
        l.centro_dep AS centro_sup,
        2 AS nivel,
        CONCAT(l.family, l.centro_dep) AS family
    FROM lvl1 AS l
    INNER JOIN {{ ref('tr_centros1') }} AS c
        ON l.centro_dep = c.centro_sup
),

lvl3 AS (
    SELECT
        c.centro_dep,
        l.centro_dep AS centro_sup,
        3 AS nivel,
        CONCAT(l.family, l.centro_dep) AS family
    FROM lvl2 AS l
    INNER JOIN {{ ref('tr_centros1') }} AS c
        ON l.centro_dep = c.centro_sup
),

lvl4 AS (
    SELECT
        c.centro_dep,
        l.centro_dep AS centro_sup,
        4 AS nivel,
        CONCAT(l.family, l.centro_dep) AS family
    FROM lvl3 AS l
    INNER JOIN {{ ref('tr_centros1') }} AS c
        ON l.centro_dep = c.centro_sup
),

lvl5 AS (
    SELECT
        c.centro_dep,
        l.centro_dep AS centro_sup,
        5 AS nivel,
        CONCAT(l.family, l.centro_dep) AS family
    FROM lvl4 AS l
    INNER JOIN {{ ref('tr_centros1') }} AS c
        ON l.centro_dep = c.centro_sup
),

union_all_levels AS (
    SELECT * FROM lvl1
    UNION ALL
    SELECT * FROM lvl2
    UNION ALL
    SELECT * FROM lvl3
    UNION ALL
    SELECT * FROM lvl4
    UNION ALL
    SELECT * FROM lvl5
),

add_consejo_rector AS (
    SELECT * FROM union_all_levels
    UNION ALL
    SELECT
        '9100' AS centro_dep,
        '9100' AS centro_sup,
        1 AS nivel,
        '9100' AS family
)

SELECT *
FROM add_consejo_rector
