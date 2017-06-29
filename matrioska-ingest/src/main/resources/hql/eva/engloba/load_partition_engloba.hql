INSERT INTO TABLE {md_eva}.engloba PARTITION (year=$1, month=$2, day=$3)
SELECT
vendgeh1_sq.fec_fecha_s, vendgeh1_sq.cod_sede, vendgwn1_sq.des_sedecib, vendgwp1_sq.cod_paisoalf,
vendgwp1_sq.des_pais_lar, vendgwp1_sq.des_pais_usu, vendgeh1_sq.cod_cliengid, tendgcel_sq.des_cliengid,
tendggur_sq.cod_grpempre, tendggur_sq.des_grpempre, tendgwje_sq.cod_prodnv1, tendgwje_sq.des_prodnv1,
tendgwlr_sq.cod_prodnv2, tendgwlr_sq.des_prodnv2, tendgwqr_sq.cod_prodnv3, tendgwqr_sq.des_prodnv3,
tendgwbp_sq.cod_prodnv4, tendgwbp_sq.des_prodnv4, vendgeh1_sq.cod_prodnv5, tendgvdp_sq.des_prodnv5,
vendgeh1_sq.cod_tipmulre, vendgeh1_sq.cod_tipjerar, vendgeh1_sq.cod_paisprod, vendgeh1_sq.cod_midlat,
vendgeh1_sq.cod_entofic, vendgeh1_sq.cod_topergm, vendgeh1_sq.cod_indglob, vendgeh1_sq.cod_salamesa,
vendgeh1_sq.xti_indclixb, vendgeh1_sq.xti_indajus, vendgeh1_sq.cod_gestb, vendgeh1_sq.cod_gesgcc,
vendgeh1_sq.cod_cober_s, vendgeh1_sq.cod_entarea, vendgeh1_sq.cod_paisorxb, vendgeh1_sq.cod_paidesxb,
vendgeh1_sq.cod_sedeorxb, vendgeh1_sq.cod_paisny, vendgeh1_sq.cod_divori, vendgeh1_sq.cod_gestloc,
vendgeh1_sq.xti_periodic, vendgeh1_sq.xti_indicxb, vendgeh1_sq.xti_completo, vendgeh1_sq.xti_indclfic,
vendgeh1_sq.imp_renmb, vendgeh1_sq.imp_rensma, vendgeh1_sq.imp_renopos, vendgeh1_sq.imp_renmf,
vendgeh1_sq.imp_renact, vendgeh1_sq.imp_renfb, vendgeh1_sq.imp_renfn, vendgeh1_sq.imp_renvc,
vendgeh1_sq.imp_ingest, vendgeh1_sq.imp_fondgd, vendgeh1_sq.imp_rencva, vendgeh1_sq.imp_costes,
vendgeh1_sq.imp_renmbm, vendgeh1_sq.imp_rensmm, vendgeh1_sq.imp_renopom, vendgeh1_sq.imp_renmfm,
vendgeh1_sq.imp_renactm, vendgeh1_sq.imp_renfbm, vendgeh1_sq.imp_renfnm, vendgeh1_sq.imp_renvcm,
vendgeh1_sq.imp_ingestm, vendgeh1_sq.imp_fondgdm, vendgeh1_sq.imp_rencvam, vendgeh1_sq.imp_costesm,
vendgeh1_sq.imp_renmbeh, vendgeh1_sq.imp_rensmaeh, vendgeh1_sq.imp_renopoeh, vendgeh1_sq.imp_renmfeh,
vendgeh1_sq.imp_renacteh, vendgeh1_sq.imp_renfbeh, vendgeh1_sq.imp_renfneh, vendgeh1_sq.imp_renvceh,
vendgeh1_sq.imp_ingesteh, vendgeh1_sq.imp_fondgdeh, vendgeh1_sq.imp_rencvaeh, vendgeh1_sq.imp_costeseh,
vendgeh1_sq.imp_renmbmeh, vendgeh1_sq.imp_rensmmeh, vendgeh1_sq.imp_renopmeh, vendgeh1_sq.imp_renmfmeh,
vendgeh1_sq.imp_renacmeh, vendgeh1_sq.imp_renfbmeh, vendgeh1_sq.imp_renfnmeh, vendgeh1_sq.imp_renvcmeh,
vendgeh1_sq.imp_ingesmeh, vendgeh1_sq.imp_fondgmeh, vendgeh1_sq.imp_rencvmeh, vendgeh1_sq.imp_costsmeh,
vendgeh1_sq.cod_audit, vendgeh1_sq.cod_regid, vendgeh1_sq.aud_usuario, vendgeh1_sq.aud_tim, vendgeh1_sq.qnu_opergm
FROM    (SELECT * FROM {rd_informacional}.tendgehr
          WHERE  year=$1 AND month=$2 AND day=$3 AND last_version=1 and  cod_tipmulre in ('NO', 'PS', 'CM') ) AS vendgeh1_sq
JOIN    (SELECT * FROM {rd_informacional}.tendgcel WHERE year=$1 AND month=$2 AND day=$3 AND last_version=1) AS tendgcel_sq
     ON tendgcel_sq.cod_cliengid = vendgeh1_sq.cod_cliengid
JOIN    (SELECT * FROM {rd_informacional}.tendgwne WHERE xti_flag='S' AND year=$1 AND month=$2 AND day=$3 AND last_version=1) AS vendgwn1_sq
     ON vendgwn1_sq.cod_sede = VENDGEH1_SQ.cod_sede
JOIN    (SELECT * FROM {rd_informacional}.tendgwpa WHERE xti_flag='S' AND year=$1 AND month=$2 AND day=$3 AND last_version=1) AS vendgwp1_sq
     ON vendgwn1_sq.cod_paisoalf = vendgwp1_sq.cod_paisoalf
JOIN    (SELECT * FROM {rd_informacional}.tendgvdp WHERE xti_flag='S' AND year=$1 AND month=$2 AND day=$3 AND last_version=1) AS tendgvdp_sq
     ON tendgvdp_sq.cod_prodnv5 = vendgeh1_sq.cod_prodnv5
JOIN    (SELECT * FROM {rd_informacional}.tendgwbp WHERE xti_flag='S' AND year=$1 AND month=$2 AND day=$3 AND last_version=1) AS tendgwbp_sq
     ON tendgwbp_sq.cod_prodnv4 = tendgvdp_sq.cod_prodnv4
JOIN    (SELECT * FROM {rd_informacional}.tendgwqr WHERE xti_flag='S' AND year=$1 AND month=$2 AND day=$3 AND last_version=1) AS tendgwqr_sq
     ON tendgwbp_sq.cod_prodnv3 = tendgwqr_sq.cod_prodnv3
JOIN    (SELECT * FROM {rd_informacional}.tendgwlr WHERE xti_flag='S' AND year=$1 AND month=$2 AND day=$3 AND last_version=1) AS tendgwlr_sq
     ON tendgwqr_sq.cod_prodnv2 = tendgwlr_sq.cod_prodnv2
JOIN    (SELECT * FROM {rd_informacional}.tendgwje WHERE xti_flag='S' AND year=$1 AND month=$2 AND day=$3 AND last_version=1) AS tendgwje_sq
     ON tendgwlr_sq.cod_prodnv1 = tendgwje_sq.cod_prodnv1
JOIN    (SELECT * FROM {rd_informacional}.tendgcir WHERE year=$1 AND month=$2 AND day=$3 AND last_version=1) AS tendgcir_sq
     ON tendgcel_sq.cod_cliendat = tendgcir_sq.cod_cliendat
JOIN    (SELECT * FROM {rd_informacional}.tendggur WHERE year=$1 AND month=$2 AND day=$3 AND last_version=1) AS tendggur_sq
     ON tendgcir_sq.cod_grpempre = tendggur_sq.cod_grpempre