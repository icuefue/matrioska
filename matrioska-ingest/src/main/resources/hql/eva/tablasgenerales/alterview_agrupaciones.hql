CREATE OR REPLACE VIEW {md_tablasgenerales}.agrupaciones
(
    cod_grupogi COMMENT 'Codigo de Grupo Empresarial.[[17885]]'
    ,fec_ini COMMENT 'Fecha de inicio de vigencia del dato [[17992]]'
    ,cod_tipagrgi COMMENT 'Codigo de Tipo de Agrupacion. Posibles valores:1A - Grupo Patrimonial,1B - Grupo Multiparticipado'
    ,des_grupogi COMMENT 'Nombre del Grupo Empresarial.[[17953]]'
    ,fec_alta COMMENT 'FECHA DE ALTA.[[396]]'
    ,xti_riesgomu COMMENT 'Indicador de Grupo con Riesgo Multiple.[[17934]]'
    ,xti_avv COMMENT 'Indicador de Grupo con alto volumende ventas (AVV).[[17933]]'
    ,xti_ifi COMMENT 'Indicador de Grupo IFI.[[17936]]'
    ,xti_soberano COMMENT 'Indicador de Grupo Soberano.[[17939]]'
    ,xti_sanciona COMMENT 'Indicador de Grupo sancionado por CRG o superior.[[17938]]'
    ,xti_critdisc COMMENT 'Indicador de Critero Discreccional.[[17932]]'
    ,xti_intriesg COMMENT 'Indicador de Grupo de interï¿½s para Riesgos Holding.[[17937]]'
    ,cod_paisgest COMMENT 'Codigo iso del pais de gestion.[[17895]]'
    ,cod_paisoalf COMMENT 'CODIGO DE PAIS ISO ALFABETICO.ES EL CODIGO ALFABETICO SEGUN NORMAS ISO.[[17893]]'
    ,cod_entalfa COMMENT 'CODIGO ISO DE LA ENTIDAD.SE TRATA DEL CODIGO UNIVERSAL DE LA ENTIDAD [[17960]]'
    ,xti_holdingi COMMENT 'Indicador de Grupo Holding.[[17935]]'
    ,des_pagweb COMMENT 'Pagina web del Grupo Empresarial.[[17910]]'
    ,cod_sectorgi COMMENT 'Cï¿½digo de Sector CRM.[[17897]]'
    ,cod_segmengi COMMENT 'Codigo de Segmento CRM.[[3297]]'
    ,cod_tipolgi COMMENT 'Codigo de Tipologia CRM.[[17904]]'
    ,xti_ace COMMENT 'Indicador de Grupo ACE.[[17092]]'
    ,cod_tiergi COMMENT 'Codigo de TIER CRM.[[2161]]'
    ,cod_sedegi COMMENT 'Codigo de Sede Global CRM.[[17898]]'
    ,xti_riesiste COMMENT 'Indicador de Grupo de Riesgo Sistemico.[[17948]]'
    ,fec_depuraci COMMENT 'FECHA DE DEPURACION DE LA INFORMACION.[[17923]]'
    ,xti_segenglo COMMENT 'Indicador de Grupo seguimiento Engloba. Equivale a Grupo CIB de CRM.[[17949]]'
    ,cod_indusxgi COMMENT 'Cï¿½digo de industria.[[3303]]'
    ,xti_trasptfm COMMENT 'Solicitud de traspaso del Grupo de CRM a TFM.[[17951]]'
    ,xti_bloqoper COMMENT 'Indicador de bloqueo operativo de la agrupacion debido a que se esta siendo depurado.[[17925]]'
    ,cod_convigi COMMENT 'Indicador de convivencia (aplicable exclusivamente en agrupaciones de tipo subgrupo).[[17881]]'
    ,cod_desglogi COMMENT 'Codigo de Desglose del Subsegmento.[[17882]]'
    ,cod_subseggi COMMENT 'Codigo de Subsegmento CRM.[[3299]]'
    ,cod_areanegi COMMENT 'Codigo de Tipo de Grupo por Area de Negocio. Posibles valores:L - Local ,C- CIB,B - BEC [[17901]]'
    ,fec_fin COMMENT 'Fecha de fin de vigencia del dato [[17994]]'
    ,aud_user COMMENT 'Auditoria usuario'
    ,aud_tim COMMENT 'Auditoria timestamp'
    ,cod_regid COMMENT 'Codigo de registro'
    ,cod_audit COMMENT 'Codigo de auditoria'
    ,year COMMENT 'year'
    ,month COMMENT 'month'
    ,day COMMENT 'day'
) COMMENT 'Grupos Empresariales constituidos por un conjunto de sociedades que se vinculan entre si a traves de relaciones operativas y contractuales para cumplir con unos objetivos comunes.'
AS
SELECT
    cod_grupogi
    ,fec_ini
    ,cod_tipagrgi
    ,des_grupogi
    ,fec_alta
    ,xti_riesgomu
    ,xti_avv
    ,xti_ifi
    ,xti_soberano
    ,xti_sanciona
    ,xti_critdisc
    ,xti_intriesg
    ,cod_paisgest
    ,cod_paisoalf
    ,cod_entalfa
    ,xti_holdingi
    ,des_pagweb
    ,cod_sectorgi
    ,cod_segmengi
    ,cod_tipolgi
    ,xti_ace
    ,cod_tiergi
    ,cod_sedegi
    ,xti_riesiste
    ,fec_depuraci
    ,xti_segenglo
    ,cod_indusxgi
    ,xti_trasptfm
    ,xti_bloqoper
    ,cod_convigi
    ,cod_desglogi
    ,cod_subseggi
    ,cod_areanegi
    ,fec_fin
    ,aud_user
    ,aud_tim
    ,cod_regid
    ,cod_audit
    ,year
    ,month
    ,day
FROM {rd_informacional}.tenjcgup WHERE year=$1 AND month=$2 AND day=$3 AND last_version=1