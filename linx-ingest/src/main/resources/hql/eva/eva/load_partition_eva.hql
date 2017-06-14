INSERT INTO TABLE {md_eva}.eva PARTITION (year=$1, month=$2, day=$3)
select 
cast(substring(eva.FEC_FECHA_S,1,10) as date)as fecha,
producto.cod_areapro as producto,
producto.cod_progl as subproducto,
sede.COD_DESGSEDE as sede,
eva.COD_GRUPOCRM as cod_grupo,
eva.COD_TIPINEVA as tipoEva,
eva.COD_INDDCM as ind_distOrig,
eva.XTI_MINORIT as ind_minoritarios,
eva.POR_TIPIMPO as tipo_impositivo,
eva.IMP_ING_MEEH as ingresosM,
eva.IMP_CSOP_MEH as costesOperativosM,
eva.IMP_PE_ME_EH as peM,
eva.IMP_EAD_MEH as eadM,
eva.IMP_BAI_MEH as beneficioAntesImpM,
eva.IMP_IMP_MEH as impuestosM,
eva.IMP_BESP_MEH as beneficioEconomicoM,
eva.IMP_BEAT_MEH as beneficioAtribuidoM,
eva.IMP_ROP_MEH as ccROperacionalM,
eva.IMP_CSROPM_H as costeROperacionalM,
eva.IMP_RCR_MEH as ccRCreditoM,
eva.IMP_CSCR_MEH as costeRCreditoM,
eva.IMP_CVA_MEH as ccRiesgoCVAM,
eva.IMP_CSCV_MEH as costeRiegoCVAM,
eva.IMP_RCP_MEH as ccRContrapartidaM,
eva.IMP_CS_CPMEH as costeRContrapartidaM,
eva.IMP_RMR_MEH as ccRMercadoM,
eva.IMP_CSRMR_MH as costeRMercadoM,
eva.IMP_CO_CAMEH as consumoCapitalM,
eva.IMP_CS_CAMEH as costeCapitalM,
eva.IMP_EVA_MEH as evaM,
eva.IMP_ING_AEH as ingresosA,
eva.IMP_CSOP_AEH as costesOperativosA,
eva.IMP_CS_CAEH as costeCapitalA,
eva.IMP_PE_ACUEH as peA,
eva.IMP_EAD_AEH as eadA,
eva.IMP_BAI_AEH  as beneficioAntesImpA,
eva.IMP_IMP_AEH  as impuestosA,
eva.IMP_BESP_AEH  as beneficioEconomicoA,
eva.IMP_BEAT_AEH as beneficioAtribuidoA,
eva.IMP_ROP_AEH as ccROperacionalA,
eva.IMP_CSROP_AH as costeROperacionalA,
eva.IMP_RCR_AEH as ccRCreditoA,
eva.IMP_CSRC_AEH as costeRCreditoA,
eva.IMP_CVA_AEH as ccRiesgoCVAA,
eva.IMP_CSCV_AEH as costeRiegoCVAA,
eva.IMP_RCP_AEH as ccRContrapartidaA,
eva.IMP_CSRCP_AH as costeRContrapartidaA,
eva.IMP_RMR_AEH as ccRMercadoA,
eva.IMP_CSRMR_AH as costeRMercadoA,
eva.IMP_CON_CAEH as consumoCapitalA,
eva.IMP_EVA_AEH as evaA,
sede.cod_sedeg as cod_sede,
DAY(cast(substring(eva.FEC_FECHA_S,1,10) as date)) as dia,
MONTH(cast(substring(eva.FEC_FECHA_S,1,10) as date)) as mes,
YEAR(cast(substring(eva.FEC_FECHA_S,1,10) as date)) as anio,
NULL as fechan
from
(select * from {rd_informacional}.tendvehs where year=$1 AND month=$2 AND day=$3 AND last_version=1) eva,
(select * from {rd_informacional}.tendvtcb where year=$1 AND month=$2 AND day=$3 AND last_version=1) tipoEva,
(select * from {rd_informacional}.tenwfduc where year=$1 AND month=$2 AND day=$3 AND last_version=1) producto,
(select * from {rd_informacional}.tentgges where year=$1 AND month=$2 AND day=$3 AND last_version=1) sede
where
tipoEva.XTI_CVA in ('1') and
tipoEva.XTI_DEFAULT in ('1') and
tipoEva.XTI_MERCADOS in ('1') and
eva.COD_INDDCM in ('0', '1') and
eva.XTI_MINORIT in ('0') and
eva.COD_TIPINEVA = tipoEva.COD_TIPINEVA and
eva.COD_JEPROG_S = producto.cod_jeraq_s and
eva.COD_DESGSEDE = sede.cod_sedeg
union all 
select
cast(substring(eva.FEC_FECHA_S,1,10) as date) as fecha,
producto.cod_areapro as producto,
producto.cod_progl as subproducto,
case eva.COD_DESGSEDE
  when '9901' then 'GM-EU'
  when '9902' then 'GM-AS'
END
as sede,
eva.COD_GRUPOCRM as cod_grupo,
eva.COD_TIPINEVA as tipoEva,
eva.COD_INDDCM as ind_distOrig,
eva.XTI_MINORIT as ind_minoritarios,
eva.POR_TIPIMPO as tipo_impositivo,
eva.IMP_ING_MEEH as ingresosM,
eva.IMP_CSOP_MEH as costesOperativosM,
eva.IMP_PE_ME_EH as peM,
eva.IMP_EAD_MEH as eadM,
eva.IMP_BAI_MEH as beneficioAntesImpM,
eva.IMP_IMP_MEH as impuestosM,
eva.IMP_BESP_MEH as beneficioEconomicoM,
eva.IMP_BEAT_MEH as beneficioAtribuidoM,
eva.IMP_ROP_MEH as ccROperacionalM,
eva.IMP_CSROPM_H as costeROperacionalM,
eva.IMP_RCR_MEH as ccRCreditoM,
eva.IMP_CSCR_MEH as costeRCreditoM,
eva.IMP_CVA_MEH as ccRiesgoCVAM,
eva.IMP_CSCV_MEH as costeRiegoCVAM,
eva.IMP_RCP_MEH as ccRContrapartidaM,
eva.IMP_CS_CPMEH as costeRContrapartidaM,
eva.IMP_RMR_MEH as ccRMercadoM,
eva.IMP_CSRMR_MH as costeRMercadoM,
eva.IMP_CO_CAMEH as consumoCapitalM,
eva.IMP_CS_CAMEH as costeCapitalM,
eva.IMP_EVA_MEH as evaM,
eva.IMP_ING_AEH as ingresosA,
eva.IMP_CSOP_AEH as costesOperativosA,
eva.IMP_CS_CAEH as costeCapitalA,
eva.IMP_PE_ACUEH as peA,
eva.IMP_EAD_AEH as eadA,
eva.IMP_BAI_AEH  as beneficioAntesImpA,
eva.IMP_IMP_AEH  as impuestosA,
eva.IMP_BESP_AEH  as beneficioEconomicoA,
eva.IMP_BEAT_AEH as beneficioAtribuidoA,
eva.IMP_ROP_AEH as ccROperacionalA,
eva.IMP_CSROP_AH as costeROperacionalA,
eva.IMP_RCR_AEH as ccRCreditoA,
eva.IMP_CSRC_AEH as costeRCreditoA,
eva.IMP_CVA_AEH as ccRiesgoCVAA,
eva.IMP_CSCV_AEH as costeRiegoCVAA,
eva.IMP_RCP_AEH as ccRContrapartidaA,
eva.IMP_CSRCP_AH as costeRContrapartidaA,
eva.IMP_RMR_AEH as ccRMercadoA,
eva.IMP_CSRMR_AH as costeRMercadoA,
eva.IMP_CON_CAEH as consumoCapitalA,
eva.IMP_EVA_AEH as evaA,
sede.cod_sedeg as cod_sede,
DAY(cast(substring(eva.FEC_FECHA_S,1,10) as date)) as dia,
MONTH(cast(substring(eva.FEC_FECHA_S,1,10) as date)) as mes,
YEAR(cast(substring(eva.FEC_FECHA_S,1,10) as date)) as anio,
NULL as fechan
from
(select * from {rd_informacional}.tendvehs where year=$1 AND month=$2 AND day=$3 AND last_version=1) eva,
(select * from {rd_informacional}.tendvtcb where year=$1 AND month=$2 AND day=$3 AND last_version=1) tipoEva,
(select * from {rd_informacional}.tenwfduc where year=$1 AND month=$2 AND day=$3 AND last_version=1) producto,
(select * from {rd_informacional}.tentgges where year=$1 AND month=$2 AND day=$3 AND last_version=1) sede
where
tipoEva.XTI_CVA in ('1') and
tipoEva.XTI_DEFAULT in ('1') and
tipoEva.XTI_MERCADOS in ('1') and
eva.COD_INDDCM in ('0', '1') and
eva.XTI_MINORIT in ('0') and
eva.COD_TIPINEVA = tipoEva.COD_TIPINEVA and
eva.COD_JEPROG_S = producto.cod_jeraq_s and
eva.COD_DESGSEDE in ('9901', '9902') and
eva.COD_DESGSEDE = sede.cod_sedeg
