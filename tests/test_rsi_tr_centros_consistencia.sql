select *
    from {{ ref('rsi_tr_centros') }}
where nomb_localidad_ag = 'GRANADAAAAA' and cod_provincia_ag <> '18'