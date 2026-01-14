{{ config(
    materialized = 'table',
    tags = ['gold','centros'],
) }}

WITH rel AS (
    SELECT
        a.centro_dep,
        b.nomb_cent_uo AS nomcentro_dep,
        b.cod_mdldad_cent AS cod_mdldad_cent_dep,
        a.centro_sup, 
        c.nomb_cent_uo AS nomcentro_sup, 
        c.cod_mdldad_cent AS cod_mdldad_cent_sup,        
        CASE
            WHEN b.cod_mdldad_cent IN ('05', '06') THEN 'O'
            WHEN b.cod_mdldad_cent IN ('03', '04') THEN 'Z'
            ELSE 'D'
        END AS todos_o_ofic,
        CASE
            WHEN a.centro_sup IN ('9202', '9300', '9220', '9690', '9360', '9321', '9500')
                 AND b.cod_mdldad_cent NOT IN ('05', '06')
            THEN 'A'
            WHEN a.centro_sup IN ('9202', '9300', '9220', '9690', '9360', '9321', '9500')
                 AND a.nivel = 1
            THEN 'A'
            WHEN a.centro_sup IN ('9100', '9200') AND a.nivel = 1
            THEN 'A'
            WHEN c.cod_mdldad_cent IN ('03', '04')
                 AND b.cod_mdldad_cent IN ('05', '06')
            THEN 'Z'
            ELSE ''
        END AS ind_area_zona,
        CASE
            WHEN b.cod_mdldad_cent IN ('05', '06') AND c.cod_mdldad_cent IN ('03', '04')
            THEN a.centro_sup
            WHEN b.cod_mdldad_cent IN ('05', '06')
            THEN ''
            WHEN a.centro_sup IN ('9202', '9300', '9220', '9690', '9360', '9321', '9500')
            THEN a.centro_sup
            WHEN a.centro_dep IN ('9200', '9202', '9300', '9220', '9690', '9360', '9321', '9500')
                 AND a.nivel = 1
            THEN a.centro_dep
            WHEN a.centro_sup IN ('9100', '9200') AND a.nivel = 1
            THEN a.centro_sup
            ELSE ''
        END AS num_area,
        CASE
            WHEN b.cod_mdldad_cent IN ('05', '06')
                 AND c.cod_mdldad_cent IN ('03', '04')
            THEN c.nomb_cent_uo
            WHEN b.cod_mdldad_cent IN ('05', '06')
            THEN ''
            WHEN a.centro_sup IN ('9202', '9300', '9220', '9690', '9360', '9321', '9500')
            THEN c.nomb_cent_uo
            WHEN a.centro_dep IN ('9200', '9202', '9300', '9220', '9690', '9360', '9321', '9500')
                 AND a.nivel = 1
            THEN b.nomb_cent_uo
            WHEN a.centro_dep = '9100'
            THEN 'presidencia'
            WHEN a.centro_sup IN ('9100', '9200')
                 AND a.nivel = 1
            THEN c.nomb_cent_uo
            ELSE ''
        END AS nombre_area,
        a.family,
        CASE
            WHEN a.centro_dep IN ('9202', '9300', '9220', '9690', '9360', '9321', '9500') THEN 'V'
            WHEN a.centro_dep = '9200' THEN 'G'
            WHEN a.centro_dep = '9100' THEN 'C'
            WHEN b.cod_mdldad_cent IN ('05', '06') THEN 'O'
            WHEN b.cod_mdldad_cent IN ('03', '04') THEN 'Z'
            WHEN f.centro_plantilla IS NOT NULL THEN 'D'
            ELSE ''
        END AS tipo_cent_dep_raw,
        COALESCE(e.n_emple_asig, 0) AS n_emple_asig,
        a.nivel
    FROM {{ ref('tr_rel_centros2') }} AS a
    JOIN {{ ref('tr_centros') }} AS b
        ON a.centro_dep = b.cod_interno_uo
    JOIN {{ ref('tr_centros') }} AS c
        ON a.centro_sup = c.cod_interno_uo
    JOIN (
        SELECT
            centro_dep,
            MAX(nivel) AS max_nivel
        FROM {{ ref('tr_rel_centros2') }}
        GROUP BY centro_dep
    ) AS d
        ON a.centro_dep = d.centro_dep
    LEFT JOIN (
        SELECT
            centro_asig,
            COUNT(*) AS n_emple_asig
        FROM {{ source('centros', 'names_empleados') }}
        WHERE email <> ''
        GROUP BY centro_asig
    ) AS e
        ON a.centro_dep = e.centro_asig
    LEFT JOIN (
        SELECT DISTINCT centro_plantilla
        FROM {{ source('centros', 'names_empleados') }}
        WHERE puesto_plantilla = 'Responsable de Departamento'
    ) AS f
        ON a.centro_dep = f.centro_plantilla
),

tipos_no_vacios AS (
    SELECT DISTINCT
        centro_dep,
        tipo_cent_dep_raw AS tipo_cent_dep
    FROM rel
    WHERE tipo_cent_dep_raw <> ''
),

rel_completado AS (
    SELECT
        r.*,
        CASE
            WHEN r.tipo_cent_dep_raw <> '' THEN r.tipo_cent_dep_raw
            ELSE t.tipo_cent_dep
        END AS tipo_cent_dep
    FROM rel AS r
    LEFT JOIN tipos_no_vacios AS t
        ON r.centro_dep = t.centro_dep
)

SELECT
    centro_dep,
    nomcentro_dep,
    cod_mdldad_cent_dep,
    todos_o_ofic,
    centro_sup,
    nomcentro_sup,
    cod_mdldad_cent_sup,
    ind_area_zona,
    num_area,
    nombre_area,
    family,
    tipo_cent_dep,
    n_emple_asig,
    nivel
FROM rel_completado
