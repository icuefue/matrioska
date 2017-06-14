INSERT INTO TABLE {md_tablasgenerales}.clientes_int PARTITION (year=$1, month=$2, day=$3)
SELECT
cod_clintbdi, fec_fin_vig, des_nomcort1, des_nomcort2, des_nomclint, cod_tipins,
xti_bancario, cod_bancobdi, xti_idiapl_c, cod_plazabdi, des_plazabdi, cod_bancotes,
cod_plazates, xti_tipcltes, xti_clienifi, cod_paisifi, cod_zoifibdi, xti_estcliei,
cod_bic, xti_estbic, xti_claswift, xti_clatelex, des_departam, des_calle, des_displaza,
des_provpais, cod_oficibdi, xti_cliereal, cod_clicorp, cod_brokerws, cod_paisbdi,
fec_fmodific, cod_usuclint, cod_cclien, cod_cclienor, xti_tiposbic, cod_clitesor,
aud_user, aud_tim, cod_regid, cod_audit, year, month, day
FROM {rd_informacional}.tenydgvc
WHERE year=$1 AND month=$2 AND day=$3 AND last_version=1