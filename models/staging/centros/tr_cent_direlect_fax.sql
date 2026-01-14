{{ config(
    materialized='view',
    tags=['silver', 'centros'],
) }}

WITH cent_direct_fax AS (
    SELECT
        cod_interno_uo,
        num_dir,
        valor_elctr_dr_text,
        txt_elctr_dr,
        txt_elctr_dr_ampl,
        ROW_NUMBER() OVER (
            PARTITION BY cod_interno_uo
            ORDER BY num_dir
        ) AS cont
    FROM {{ ref('tr_centros_direlect') }}
    WHERE cod_dir_elctr = '01'
)

SELECT
    cod_interno_uo,
    num_dir,
    valor_elctr_dr_text,
    txt_elctr_dr,
    txt_elctr_dr_ampl,
    cont
FROM cent_direct_fax
