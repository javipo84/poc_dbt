{{ config(
    materialized='view',
    tags=['silver', 'centros'],
) }}

WITH tel_fax AS (
    SELECT
        cod_interno_uo,
        ind_direc,
        num_dir,
        valor_elctr_dr_text,
        txt_elctr_dr,
        txt_elctr_dr_ampl,
        ROW_NUMBER() OVER (
            PARTITION BY cod_interno_uo, ind_direc
            ORDER BY num_dir
        ) AS cont
    FROM {{ ref('tr_centros_direlect') }}
    WHERE cod_dir_elctr = '08'
)

SELECT
    cod_interno_uo,
    ind_direc,
    num_dir,
    valor_elctr_dr_text,
    txt_elctr_dr,
    txt_elctr_dr_ampl,
    cont
FROM tel_fax
