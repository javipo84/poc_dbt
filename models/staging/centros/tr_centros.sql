{{ config(
    materialized='view',
    tags=['silver', 'centros']
) }}

WITH mi_centro AS (
    SELECT
        cod_interno_uo,
        nomb_cent_uo,
        cod_provincia_ag,
        cod_municipio_ag,
        fecha_inic_ecv,
        cod_mdldad_cent,
        id_interno_pe,
        num_dir,
        mi_fecha_inic
    FROM {{ source('centros', 'mi_centro') }}
    WHERE
        mi_fecha_fin = '9999-12-31'
        AND cod_nrbe_en = '3023'
        AND cod_ecv_cent = '1'
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY cod_interno_uo
            ORDER BY mi_fecha_inic DESC
        ) = 1
),

mi_domicilio AS (
    SELECT
        cod_postal_ag,
        nomb_50,
        num_tlfno_domic,
        nomb_localidad_ag,
        mi_otro_dom,
        num_dir,
        mi_fecha_inic
    FROM {{ source('centros', 'mi_domicilio') }}
    WHERE
        mi_fecha_fin = '9999-12-31'
        AND cod_nrbe_en = '3023'
    QUALIFY
        ROW_NUMBER() OVER (
            PARTITION BY num_dir
            ORDER BY mi_fecha_inic DESC
        ) = 1
)

SELECT
    c.cod_interno_uo,
    c.nomb_cent_uo,
    c.cod_provincia_ag,
    c.cod_municipio_ag,
    c.fecha_inic_ecv,
    c.cod_mdldad_cent,
    c.id_interno_pe,
    d.cod_postal_ag,
    d.nomb_50,
    d.num_tlfno_domic,
    d.nomb_localidad_ag,
    d.mi_otro_dom
FROM mi_centro AS c
INNER JOIN mi_domicilio AS d
    ON c.num_dir = d.num_dir
