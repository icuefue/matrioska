INSERT INTO TABLE {md_tablasgenerales}.clientes PARTITION (year=$1, month=$2, day=$3)
SELECT
cod_icclient, cod_paisint, cod_bancsb_r, cod_cclien,
xti_tiperso, fec_faltaf, fec_fmodific, cod_ctipcl,
cod_docalf, cod_ctipcls, qnu_xfodni, xti_fotoc,
cod_cdnombre, des_denomb, xti_cdomic, cod_ctpvia,
des_ccalle, qnu_cnuvia, des_cresto, des_dplaza,
des_dprovi, cod_cdipos, cod_provfi, cod_cdipex,
cod_cpaisn, des_mail, cod_qtfnop, cod_qtfnot,
cod_ccnonac, xti_cclocu, xti_cperso,
cod_tipins, cod_critins, xti_csecli, cod_soccon,
xti_cclemp, cod_bgempl, cod_ctrata, xti_cecivi, xti_csexof,
xti_cregma, des_dlugna, des_dprona, cod_cplaza, cod_clpana, fec_fnacif,
fec_freplf, cod_paisnad, xti_cforma, cod_sector, cod_subsectr,
cod_forsoci, xti_forsoci, xti_cvip, cod_confiden, xti_ctecom,
xti_clerror, cod_perfila, xti_bloqueo, xti_refundic, xti_fic,
xti_con, xti_dic, xti_dcc, xti_nrc, xti_ilc, xti_dac, xti_for,
xti_firmad, xti_proacm, cod_ccltse, xti_sidebe, cod_gruporie,
cod_swift, xti_finact, xti_morabbva, xti_moragrup, xti_asne, xti_indjud,
xti_rai, xti_caba, xti_finanza, cod_idiomcor, cod_idioma_i,
cod_tipinucr, cod_sitconc, xti_empexret, fec_iniexret, fec_empexret,
cod_cgesto, cod_vialta, xti_vialta, xti_tiporol, xti_masconfp, cod_nivconfi,
xti_masdtgdc, xti_masdtctp, xti_masdtpto, xti_masdtrel, xti_masdtseg,
xti_masdtpcl, xti_masdtddo, cod_cnae, cod_ofimayor, fec_fin_vig, cod_regid,
cod_audit, aud_usuario, aud_tim, year, month, day
FROM {rd_informacional}.tenyddcb
WHERE year=$1 AND month=$2 AND day=$3 AND last_version=1