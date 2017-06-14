INSERT  INTO TABLE {md_tablasgenerales}.grupo PARTITION (year=$1, month=$2, day=$3)
select
grupos.cod_grupocrm as cod_grupo,
grupos.des_grupcrm as des_grupo,
grupos.cod_user as gestor,
trim(tipologia.des_tiplg) as tipologia,
trim(tier.des_tiercrm) as tier,
trim(pais.des_pais_cor) as paisOrigen,
pais.cod_paisoalf as cod_paisOrigen,
trim(segmento.des_segcrmp) as segmento,
trim(subsegmento.des_ssegcrms) as subsegmento,
trim(sedeGestion.cod_desgsede) as sedeGestion,
sedeGestion.cod_sedeg as cod_sedeGestion,
trim(industria.des_industri) as industria,
industria.cod_induscrm as cod_industria,
grupos.xti_grupoace as grupoACE,
grupos.xti_multigru as multigrupo,
grupos.cod_docfis as docfis,
grupos.cod_sectcrm as cod_actividad,
trim(actividad.des_sectcrm) as actividad,
trim(paisgestion.des_pais_cor) as paisGestion,
paisgestion.cod_paisoalf as cod_paisGestion,
case substring(grupos.fec_fin_s,1,10) 
  when '31-12-9999' then 'N'
  else 'S'
END as baja,
case substring(grupos.fec_fin_s,1,10)
  when '31-12-9999' then null
  else grupos.fec_fin_s
END as fechaBaja
from
{rd_informacional}.tenydgup grupos join
  ( select cod_grupocrm, max(fec_fin_s) as fec_fin_s
    from {rd_informacional}.tenydgup where year=$1 AND month=$2 AND day=$3 AND last_version=1
    group by cod_grupocrm
   ) maxFechaGrupo
   on maxFechaGrupo.cod_grupocrm = grupos.cod_grupocrm
   and maxFechaGrupo.fec_fin_s = grupos.fec_fin_s
left join (select * from {rd_informacional}.TENYDYTP where year=$1 AND month=$2 AND day=$3 AND last_version=1) tipologia on tipologia.cod_tiplg_s = grupos.cod_tiplg_s
left join (select * from {rd_informacional}.TENYDIER where year=$1 AND month=$2 AND day=$3 AND last_version=1) tier on tier.cod_tier_s = grupos.cod_tier_s
left join (select * from {rd_informacional}.TENYDPAI where year=$1 AND month=$2 AND day=$3 AND last_version=1) pais on pais.cod_pais_s = grupos.cod_pais_s
left join (select * from {rd_informacional}.TENYDPAI where year=$1 AND month=$2 AND day=$3 AND last_version=1) paisGestion on paisGestion.cod_paisoalf = grupos.cod_pgestion
left join (select * from {rd_informacional}.TENYDYPS where year=$1 AND month=$2 AND day=$3 AND last_version=1) segmento on segmento.cod_segcrm_s = grupos.cod_segcrm_s
left join (select * from {rd_informacional}.TENYDYSS where year=$1 AND month=$2 AND day=$3 AND last_version=1) subsegmento on subsegmento.cod_ssgcrm_s = grupos.cod_ssgcrm_s
left join (select * from {rd_informacional}.TENYDXED where year=$1 AND month=$2 AND day=$3 AND last_version=1) sedeGestion on sedeGestion.cod_geosed_s = grupos.cod_sedege_s
left join (select * from {rd_informacional}.TENYDCRS where year=$1 AND month=$2 AND day=$3 AND last_version=1) actividad on actividad.cod_sectcrm = grupos.cod_sectmer
left join (select * from {rd_informacional}.TENYDIUR where year=$1 AND month=$2 AND day=$3 AND last_version=1) industria
on (industria.COD_INDUSCRM = grupos.cod_induscrm and substring(industria.fec_fin_s,1,10)='31-12-9999')