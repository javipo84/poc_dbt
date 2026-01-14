{{ config(
    materialized='view',
    tags=['silver','centros'],
) }}

WITH tel_oficinas AS (
    SELECT
        cod_interno_uo,
        num_dir,
        valor_elctr_dr_text,
        txt_elctr_dr,
        txt_elctr_dr_ampl,
        CASE
            WHEN txt_elctr_dr = '' THEN ''
            WHEN UPPER(SUBSTR(txt_elctr_dr, 1, 7)) = 'INTEGRA' THEN CONCAT('2', cod_interno_uo)
            WHEN LENGTH(TRIM(valor_elctr_dr_text)) = 7 THEN valor_elctr_dr_text
            WHEN SUBSTR(txt_elctr_dr, 1, 1) IN ('3', '2') THEN SUBSTR(txt_elctr_dr, 1, 7)
            ELSE ''
        END AS telefono_ext,
        CASE
            WHEN LENGTH(TRIM(valor_elctr_dr_text)) = 7 THEN ''
            ELSE valor_elctr_dr_text
        END AS telefono_num,
        CASE
            WHEN SUBSTR(txt_elctr_dr, 1, 1) IN ('3', '2') THEN SUBSTR(txt_elctr_dr, 9, 22)
            ELSE txt_elctr_dr
        END AS telefono_txt,
        ROW_NUMBER() OVER (
            PARTITION BY cod_interno_uo
            ORDER BY num_dir
        ) AS cont
    FROM {{ ref('tr_centros_direlect') }}
    WHERE cod_dir_elctr = '02'
)

SELECT
    cod_interno_uo,
    num_dir,
    valor_elctr_dr_text,
    txt_elctr_dr,
    txt_elctr_dr_ampl,
    telefono_ext,
    telefono_num,
    telefono_txt,
    cont
FROM tel_oficinas
