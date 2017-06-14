CREATE OR REPLACE VIEW {md_tablasgenerales}.participe_grupo_crm
(
    cod_cclien COMMENT 'Codigo de cliente de Clientela. [[17878]]'
    ,cod_grupogi COMMENT 'Codigo de Grupo Empresarial. [[17885]]'
    ,fec_ini COMMENT 'Fecha de inicio de vigencia del dato. [[17992]]'
    ,cod_tiporel COMMENT 'Codigo de Tipo de Relacion. Posibles valores:P - Patrimonial,E - Economica,G - Gestion. [[17903]]'
    ,cod_claserel COMMENT 'Codigo de clase de relacion. Clase de relacion establecida entre el participe y grupo con el fin de incorporar un nivel mas de detalle en la definicion de la misma. [[17877]]'
    ,fec_alta COMMENT 'FECHA DE ALTA. [[17920]]'
    ,xti_integlob COMMENT 'Indicador de vision de interes Global. [[17940]]'
    ,xti_intenego COMMENT 'Indicador de vision de interes Negocio. Indica si la asignacion Grupo-Participe se gestiona desde el area de Negocio. [[17942]]'
    ,xti_interies COMMENT 'Indicador de vision de interes Riesgos. Indica si la asignacion Grupo-Participe se gestiona desde el area de Riesgos. [[17943]]'
    ,xti_relprine COMMENT 'Indicador de Relacion Principal para Negocio. [[17946]]'
    ,xti_relpriri COMMENT 'Indicador de Relacion Principal para Riesgos. [[17947]]'
    ,xti_caberies COMMENT 'Indicador de cabecera para Riesgos. Participe principal para el grupo con el que establece la relacion, siendo el grupo de interes para el area de Riesgos. [[17928]]'
    ,xti_cabenego COMMENT 'Indicador de cabecera para Negocio. Participe principal para el grupo con el que establece la relacion, siendo el grupo de interes para el area de Negocio. [[17926]]'
    ,xti_cabepatr COMMENT 'Indicador de cabecera Patrimonial. Se asigna en funcion de la informacion que el Grupo presenta en su memoria. [[17927]]'
    ,por_efectpar COMMENT 'Porcentaje efectivo de participacion. Porcentaje de participacion del Grupo en el Participe.[[17958]]'
    ,por_compuneg COMMENT 'Porcentaje a partir del cual se imputa la rentabilidad o posiciones con interes Negocio. [[17956]]'
    ,por_compurie COMMENT 'Porcentaje a partir del cual se imputa la rentabilidad o posiciones con interes Riesgos. [[17957]]'
    ,fec_fin COMMENT 'Fecha de fin de vigencia del dato [[17994]]'
    ,aud_user COMMENT 'Auditoria usuario'
    ,aud_tim COMMENT 'Auditoria timestamp'
    ,cod_regid COMMENT 'Codigo de registro'
    ,cod_audit COMMENT 'Codigo de auditoria'
    ,year COMMENT 'year'
    ,month COMMENT 'month'
    ,day COMMENT 'day'
) COMMENT 'Relaciones establecidas entre Participes y Grupos correspondientes a CRM Plus'
AS
SELECT
    cod_cclien
    ,cod_grupogi
    ,fec_ini
    ,cod_tiporel
    ,cod_claserel
    ,fec_alta
    ,xti_integlob
    ,xti_intenego
    ,xti_interies
    ,xti_relprine
    ,xti_relpriri
    ,xti_caberies
    ,xti_cabenego
    ,xti_cabepatr
    ,por_efectpar
    ,por_compuneg
    ,por_compurie
    ,fec_fin
    ,aud_user
    ,aud_tim
    ,cod_regid
    ,cod_audit
    ,year
    ,month
    ,day
FROM {rd_informacional}.tenjcrhg WHERE year=$1 AND month=$2 AND day=$3 AND last_version=1