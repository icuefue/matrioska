CREATE OR REPLACE VIEW {md_tablasgenerales}.tipo_agrupacion
(
    cod_tipagrgi COMMENT 'Codigo de Tipo de Agrupacion. Posibles valores: 1A - Grupo Patrimonial,1B - Grupo Multiparticipado,1C - Grupo De Gestiï¿½n,2 - Supragrupo, 3 - Subgrupo [[17899]]'
    ,fec_ini COMMENT 'Fecha de inicio de vigencia del dato [[17912]]'
    ,des_tipagrgi COMMENT 'Descripcion del Tipo de Agrupacion. [[17912]]'
    ,cod_traducc COMMENT 'CODIGO TRADUCCION'
    ,xti_baja COMMENT 'Indicador de baja. [[17912]]'
    ,fec_fin COMMENT 'Fecha de fin de vigencia del dato [[17994]]'
    ,aud_user COMMENT 'Auditoria usuario'
    ,aud_tim COMMENT 'Auditoria timestamp'
    ,cod_regid COMMENT 'Codigo de registro'
    ,cod_audit COMMENT 'Codigo de auditoria'
    ,year COMMENT 'year'
    ,month COMMENT 'month'
    ,day COMMENT 'day'
) COMMENT 'Catalogo de Tipos de Agrupacion'
AS
SELECT
    cod_tipagrgi
    ,fec_ini
    ,des_tipagrgi
    ,cod_traducc
    ,xti_baja
    ,fec_fin
    ,aud_user
    ,aud_tim
    ,cod_regid
    ,cod_audit
    ,year
    ,month
    ,day
FROM {rd_informacional}.tenjctag WHERE year=$1 AND month=$2 AND day=$3 AND last_version=1