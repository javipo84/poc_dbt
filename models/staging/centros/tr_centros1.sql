{{ config(
    materialized='view',
    tags=['silver', 'centros'],
) }}

WITH rel_centros AS (
    SELECT
        cod_interno_uo_1 AS centro_dep,
        cod_interno_uo AS centro_sup
    FROM {{ source('centros', 'mi_cent_rl_cent') }}
    WHERE
        fecha_fin_act_uo = '9999-12-31'
        AND mi_fecha_fin = '9999-12-31'
        AND cod_nrbe_en = '3023'
        AND cod_cent_rl_cent = '01'
),

agregado AS (
    SELECT
        centro_dep,
        MAX(centro_sup) AS centro_sup
    FROM rel_centros
    GROUP BY centro_dep
)

SELECT
    centro_dep,
    centro_sup
FROM agregado
