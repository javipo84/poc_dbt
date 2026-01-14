{{ config(
    materialized = 'table',
    tags = ['gold','centros'],
) }}

WITH b AS (
    SELECT DISTINCT
        centro_dep,
        centro_sup,
        nomcentro_sup,
        tipo_cent_dep,
        num_area,
        nombre_area
    FROM {{ ref('rsi_jerarquias_centros') }}
    WHERE ind_area_zona <> ''
),

sup AS (
    SELECT DISTINCT
        centro_dep,
        centro_sup,
        nomcentro_sup,
        cod_mdldad_cent_sup
    FROM {{ ref('rsi_jerarquias_centros') }}
    WHERE nivel = 1
),

c_princ AS (
    SELECT *
    FROM {{ ref('tr_cent_direlect_email') }}
    WHERE ind_direc = '01' AND cont = 1
),

c_secun AS (
    SELECT *
    FROM {{ ref('tr_cent_direlect_email') }}
    WHERE ind_direc = '00' AND cont = 1
),

d AS (
    SELECT *
    FROM {{ ref('tr_cent_direlect_fax') }}
    WHERE cont = 1
),

e AS (
    SELECT *
    FROM {{ ref('tr_cent_direlect_fax') }}
    WHERE cont = 2
),

f1 AS (
    SELECT *
    FROM {{ ref('tr_cent_direlect_teldom') }}
    WHERE cont = 1
),

f2 AS (
    SELECT *
    FROM {{ ref('tr_cent_direlect_movil') }}
    WHERE cont = 1
      AND SUBSTR(txt_elctr_dr,1,1) = '4'
),

g AS (
    SELECT *
    FROM {{ ref('tr_cent_direlect_tel') }}
    WHERE cont = 1
),

h AS (
    SELECT *
    FROM {{ ref('tr_cent_direlect_tel') }}
    WHERE cont = 2
),

i AS (
    SELECT *
    FROM {{ ref('tr_cent_direlect_tel') }}
    WHERE cont = 3
)

SELECT
    a.cod_interno_uo AS centro,
    a.nomb_cent_uo AS nomcentro,
    CASE 
        WHEN sup.cod_mdldad_cent_sup IS NULL OR sup.cod_mdldad_cent_sup NOT IN ('03','04')
        THEN '0000'
        ELSE sup.centro_sup
    END AS zona,
    CASE 
        WHEN sup.cod_mdldad_cent_sup IS NULL OR sup.cod_mdldad_cent_sup NOT IN ('03','04')
        THEN 'Centros sin zona'
        ELSE sup.nomcentro_sup
    END AS nomzona,
    CASE
        WHEN sup.centro_sup = '9630' THEN 'O'
        WHEN sup.centro_sup = '9631' THEN 'SI'
        WHEN sup.centro_sup = '9632' THEN 'MA'
        WHEN sup.centro_sup = '9633' THEN 'VE'
        WHEN sup.centro_sup = '9634' THEN 'VA'
        WHEN sup.centro_sup = '9635' THEN 'S'
        WHEN sup.centro_sup = '9636' THEN 'E'
        WHEN sup.centro_sup = '9637' THEN 'N'
        WHEN sup.centro_sup = '9638' THEN 'D'
        ELSE ''
    END AS nombrezona,
    CASE
        WHEN a.cod_mdldad_cent IN ('05','06') THEN 'O'
        WHEN a.cod_mdldad_cent IN ('03','04') THEN 'Z'
        WHEN a.cod_interno_uo IN ('9685','9686','9687','1000') 
             OR a.nomb_cent_uo = 'DISPONIBLE'
             OR (a.cod_interno_uo >= '0900' AND a.cod_interno_uo <= '0999') THEN 'F'
        WHEN a.cod_mdldad_cent = '10' AND (
                 (a.cod_interno_uo >= '7000' AND a.cod_interno_uo <= '7999')
                 OR a.cod_interno_uo IN ('8001','8010','8408')
                 OR a.nomb_cent_uo LIKE 'CAJERO%'
                 OR (a.cod_interno_uo >= '9700' AND a.cod_interno_uo <= '9799')
             )
        THEN 'C'
        ELSE 'D'
    END AS todos_o_ofic,
    a.cod_mdldad_cent,
    COALESCE(sup.centro_sup,'') AS centro_sup,
    COALESCE(sup.nomcentro_sup,'') AS nomcentro_sup,
    COALESCE(b.num_area,'') AS num_area,
    COALESCE(b.nombre_area,'') AS nombre_area,
    COALESCE(b.tipo_cent_dep,'') AS tipo_cent_dep,
    COALESCE(c_princ.txt_elctr_dr_ampl,'') AS e_mail,
    COALESCE(REPLACE(c_secun.txt_elctr_dr_ampl,'@c.es','@crgranada.com'),'') AS e_mail_alias,
    a.id_interno_pe AS interno_pe,
    a.nomb_localidad_ag,
    a.cod_provincia_ag,
    a.cod_municipio_ag,
    a.cod_postal_ag,
    a.nomb_50 AS domic_50,
    COALESCE(f1.valor_elctr_dr_text,'') AS telefono,
    a.mi_otro_dom AS otros_datos,
    a.fecha_inic_ecv,
    CASE
        WHEN a.cod_mdldad_cent IN ('05','06') AND cat.categoria IS NOT NULL
        THEN 'CATEGORIA ' || cat.categoria
        ELSE ''
    END AS categoria,
    CASE
        WHEN d.txt_elctr_dr IS NULL THEN ''
        WHEN d.txt_elctr_dr = '' THEN '5' || a.cod_interno_uo
        ELSE ''
    END AS fax_ext_1,
    COALESCE(d.valor_elctr_dr_text,'') AS fax_tel_1,
    CASE
        WHEN d.txt_elctr_dr IS NULL THEN ''
        WHEN SUBSTR(d.txt_elctr_dr,1,1) = '5' THEN SUBSTR(d.txt_elctr_dr,9,22)
        ELSE d.txt_elctr_dr
    END AS fax_txt_1,
    CASE
        WHEN e.txt_elctr_dr IS NULL THEN ''
        WHEN e.txt_elctr_dr = '' THEN ''
        WHEN SUBSTR(e.txt_elctr_dr,1,1) = '5' THEN SUBSTR(e.txt_elctr_dr,1,7)
        ELSE ''
    END AS fax_ext_2,
    COALESCE(e.valor_elctr_dr_text,'') AS fax_tel_2,
    CASE
        WHEN e.txt_elctr_dr IS NULL THEN ''
        WHEN SUBSTR(e.txt_elctr_dr,1,1) = '5' THEN SUBSTR(e.txt_elctr_dr,9,22)
        ELSE e.txt_elctr_dr
    END AS fax_txt_2,
    CASE
        WHEN f2.txt_elctr_dr IS NULL THEN ''
        WHEN f2.txt_elctr_dr = '' THEN ''
        WHEN SUBSTR(f2.txt_elctr_dr,1,1) = '4' THEN SUBSTR(f2.txt_elctr_dr,1,7)
        ELSE ''
    END AS movil_ext_1,
    COALESCE(f2.valor_elctr_dr_text,'') AS movil_num_1,
    CASE
        WHEN f2.txt_elctr_dr IS NULL THEN ''
        WHEN SUBSTR(f2.txt_elctr_dr,1,1) = '4' THEN SUBSTR(f2.txt_elctr_dr,9,22)
        ELSE f2.txt_elctr_dr
    END AS movil_txt_1,
    COALESCE(g.telefono_ext,'') AS telefono_ext_1,
    COALESCE(g.telefono_num,'') AS telefono_num_1,
    COALESCE(g.telefono_txt,'') AS telefono_txt_1,
    COALESCE(h.telefono_ext,'') AS telefono_ext_2,
    COALESCE(h.telefono_num,'') AS telefono_num_2,
    COALESCE(h.telefono_txt,'') AS telefono_txt_2,
    COALESCE(i.telefono_ext,'') AS telefono_ext_3,
    COALESCE(i.telefono_num,'') AS telefono_num_3,
    COALESCE(i.telefono_txt,'') AS telefono_txt_3
FROM {{ ref('tr_centros') }} AS a
LEFT JOIN b ON a.cod_interno_uo = b.centro_dep
LEFT JOIN sup ON a.cod_interno_uo = sup.centro_dep
LEFT JOIN {{ source('centros','categoria_oficinas_2025') }} AS cat ON CAST(a.cod_interno_uo AS INT64) = cat.num_ofi
LEFT JOIN c_princ ON a.cod_interno_uo = c_princ.cod_interno_uo
LEFT JOIN c_secun ON a.cod_interno_uo = c_secun.cod_interno_uo
LEFT JOIN d ON a.cod_interno_uo = d.cod_interno_uo
LEFT JOIN e ON a.cod_interno_uo = e.cod_interno_uo
LEFT JOIN f1 ON a.cod_interno_uo = f1.cod_interno_uo
LEFT JOIN f2 ON a.cod_interno_uo = f2.cod_interno_uo
LEFT JOIN g ON a.cod_interno_uo = g.cod_interno_uo
LEFT JOIN h ON a.cod_interno_uo = h.cod_interno_uo
LEFT JOIN i ON a.cod_interno_uo = i.cod_interno_uo