CREATE OR REPLACE VIEW {md_tablasgenerales}.rela_agrupaciones
(
    cod_grupogi COMMENT 'Codigo de Grupo Empresarial.[[17885]]'
    ,cod_grupohi COMMENT 'Codigo de Grupo Empresarial Hijo.[[17890]]'
    ,fec_ini COMMENT 'Fecha de inicio de vigencia del dato.[[17992]]'
    ,cod_tiporel COMMENT 'Codigo de Tipo de Relacion.[[17903]]'
    ,cod_claserel COMMENT 'Codigo de clase de relacion. Clase de relaciï¿½n establecida entre el participe y grupo. Depende del tipo de relacion.[[17877]]'
    ,fec_alta COMMENT 'FECHA DE ALTA.[[17921]]'
    ,xti_integlob COMMENT 'Indicador de vision de interes Global.[[17941]]'
    ,xti_intenego COMMENT 'Indicador de vision de interes Negocio. Indica si la asignacion Grupo-Participe se gestiona desde el area de Negocio. [[17942]]'
    ,xti_interies COMMENT 'Indicador de vision de interes Riesgos. Indica si la asignacion Grupo-Participe se gestiona desde el area de Riesgos. [[17943]]'
    ,xti_relprine COMMENT 'Indicador de Relacion Principal para Negocio. [[17946]]'
    ,xti_relpriri COMMENT 'Indicador de Relacion Principal para Riesgos. [[17947]]'
    ,por_efectpar COMMENT 'Porcentaje efectivo de participacion. Porcentaje de participaciï¿½n del Grupo en el Participe. [[17958]]'
    ,por_compuneg COMMENT 'Porcentaje a partir del cual se imputa la rentabilidad o posiciones con interes Negocio. [[17956]]'
    ,por_compurie COMMENT 'Porcentaje a partir del cual se imputa la rentabilidad o posiciones con interes Riesgos. [[17957]]'
    ,fec_fin COMMENT 'Fecha de fin de vigencia del dato. [[17994]]'
    ,aud_user COMMENT 'Auditoria usuario'
    ,aud_tim COMMENT 'Auditoria timestamp'
    ,cod_regid COMMENT 'Codigo de registro'
    ,cod_audit COMMENT 'Codigo de auditoria'
    ,year COMMENT 'year'
    ,month COMMENT 'month'
    ,day COMMENT 'day'
) COMMENT 'Relaciones establecidas entre Grupos.'
AS
SELECT
    cod_grupogi
    ,cod_grupohi
    ,fec_ini
    ,cod_tiporel
    ,cod_claserel
    ,fec_alta
    ,xti_integlob
    ,xti_intenego
    ,xti_interies
    ,xti_relprine
    ,xti_relpriri
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
FROM {rd_informacional}.tenjcrkg WHERE year=$1 AND month=$2 AND day=$3 AND last_version=1