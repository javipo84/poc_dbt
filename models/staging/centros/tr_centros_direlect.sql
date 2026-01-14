{{ config(
    materialized='view',
    tags=['silver', 'centros'],
) }}

WITH clte_domic AS (
    SELECT
        cod_nrbe_en,
        num_dir,
        id_interno_pe,
        cod_dir,
        ind_direc,
        mi_fecha_inic,
        mi_fecha_fin,
        cod_pers_rl_dir
    FROM {{ source('centros', 'mi_clte_domic') }}
    WHERE
        mi_fecha_fin = '9999-12-31'
        AND cod_nrbe_en = '3023'
        AND cod_pers_rl_dir = '07'
        AND cod_dir = '2'
),

centro AS (
    SELECT
        cod_interno_uo,
        nomb_cent_uo,
        cod_mdldad_cent,
        id_interno_pe,
        mi_fecha_fin,
        cod_nrbe_en,
        cod_ecv_cent
    FROM {{ source('centros', 'mi_centro') }}
    WHERE
        mi_fecha_fin = '9999-12-31'
        AND cod_nrbe_en = '3023'
        AND cod_ecv_cent = '1'
),

dir_electr AS (
    SELECT
        cod_nrbe_en,
        num_dir,
        cod_dir_elctr,
        txt_elctr_dr_ampl,
        valor_elctr_dr_text,
        txt_elctr_dr
    FROM {{ source('centros', 'mi_dir_electr') }}
    WHERE
        cod_nrbe_en = '3023'
        AND cod_dir_elctr IN ('01', '02', '09', '08', '07')
)

SELECT
    ce.cod_interno_uo,
    cd.ind_direc,
    ce.nomb_cent_uo,
    ce.cod_mdldad_cent,
    cd.num_dir,
    cd.id_interno_pe,
    cd.cod_dir,
    cd.mi_fecha_inic,
    de.txt_elctr_dr_ampl,
    de.txt_elctr_dr,
    de.cod_dir_elctr,
    REPLACE(de.valor_elctr_dr_text, '+34', '') AS valor_elctr_dr_text
FROM clte_domic AS cd
INNER JOIN dir_electr AS de
    ON cd.cod_nrbe_en = de.cod_nrbe_en AND cd.num_dir = de.num_dir
INNER JOIN centro AS ce
    ON cd.cod_nrbe_en = ce.cod_nrbe_en AND cd.id_interno_pe = ce.id_interno_pe
GROUP BY
    ce.cod_interno_uo,
    cd.ind_direc,
    ce.nomb_cent_uo,
    ce.cod_mdldad_cent,
    cd.num_dir,
    cd.id_interno_pe,
    cd.cod_dir,
    cd.mi_fecha_inic,
    de.txt_elctr_dr_ampl,
    de.valor_elctr_dr_text,
    de.txt_elctr_dr,
    de.cod_dir_elctr
