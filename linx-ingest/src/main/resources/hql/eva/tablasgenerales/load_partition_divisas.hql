INSERT INTO TABLE {md_tablasgenerales}.divisas PARTITION (year=$1, month=$2, day=$3)
SELECT
cod_cdisoalf, fec_ini_vig, cod_idiomiso, cod_paisoalf, cod_divison, cod_divinte,
cod_divsbc, des_divisaco, des_divabr, des_diviplu, qnu_ordend, qnu_digdes,
xti_coticont, xti_dicotiza, xti_dinopera, xti_dibillet, xti_billsusp, xti_digenero,
qnu_decidi, qnu_deciint, xti_dianocom, xti_porcoti, xti_porcoant, fec_porcomod, xti_primfest, qnu_diasfes,
por_idemora, xti_dioperat, fec_divbaja, xti_converge, fec_converg, fec_finconv, xti_posidivi, fec_posidiv,
cod_traducc, cod_trad_plu, fec_fin_vig, cod_regid, cod_audit, aud_usuario, aud_tim, year, month, day
FROM {rd_informacional}.tentgdvs
WHERE year=$1 AND month=$2 AND day=$3 AND last_version=1