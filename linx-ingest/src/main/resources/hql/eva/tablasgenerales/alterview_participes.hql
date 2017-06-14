CREATE OR REPLACE VIEW {md_tablasgenerales}.participes
(
    cod_cclien COMMENT 'Codigo de cliente de Clientela. [[17878]]'
    ,fec_ini COMMENT 'Fecha de inicio de vigencia del dato [[17992]]'
    ,xti_multigru COMMENT 'Indicador de participe multigrupo. [[17945]]'
    ,xti_noclient COMMENT 'Indicador de No Cliente. Identifica los No clientes que se den de alta en CRM. [[17944]]'
    ,fec_baja COMMENT 'Fecha de baja del cliente en la aplicacion de Clientela. [[17922]]'
    ,xti_cclienre COMMENT 'Indicador de cclien reutilizado por Clientela. [[17930]]'
    ,fec_fin COMMENT 'Fecha de fin de vigencia del dato [[17994]]'
    ,aud_user COMMENT 'Auditoria usuario'
    ,aud_tim COMMENT 'Auditoria timestamp'
    ,cod_regid COMMENT 'Codigo de registro'
    ,cod_audit COMMENT 'Codigo de auditoria'
    ,year COMMENT 'year'
    ,month COMMENT 'month'
    ,day COMMENT 'day'
) COMMENT 'Tabla de clientes CRM. Clientes incorporados desde Clientela (personas juridicas) y No clientes incorporados desde el online de CRM (personas fisicas y juridicas) correspondientes a CRM Plus'
AS
SELECT
    cod_cclien
    ,fec_ini
    ,xti_multigru
    ,xti_noclient
    ,fec_baja
    ,xti_cclienre
    ,fec_fin
    ,aud_user
    ,aud_tim
    ,cod_regid
    ,cod_audit
    ,year
    ,month
    ,day
FROM {rd_informacional}.tenjcktl WHERE year=$1 AND month=$2 AND day=$3 AND last_version=1