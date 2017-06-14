CREATE OR REPLACE VIEW {md_tablasgenerales}.agrupaciones_crm_p_tfm
(
    cod_grupogi COMMENT 'Codigo de Grupo Empresarial. [[17886]]'
    ,fec_ini COMMENT 'Fecha de inicio de vigencia del dato [[17992]]'
    ,cod_procd COMMENT 'Procedencia del grupo, bien si es de CRM o de TFM [[17998]]'
    ,fec_fin COMMENT 'Fecha de fin de vigencia del dato [[17994]]'
    ,aud_user COMMENT 'Identificador del usuario que realizo una operacion con vistas a su revision por Auditoria'
    ,aud_tim COMMENT 'Contiene la fecha y hora en la que se realizo una operacion con vistas a su revision por Auditoria.'
    ,cod_regid COMMENT 'identificador unico de registro'
    ,cod_audit COMMENT 'codigo secuencial unico de auditoria'
    ,year COMMENT 'year'
    ,month COMMENT 'month'
    ,day COMMENT 'day'
) COMMENT 'Tabla comun para completar el catalogo de grupos que se compone de los datos comunes entre las los grupos de CRM Plus ylos grupos TFM.'
AS
SELECT
    cod_grupogi
    ,fec_ini
    ,cod_procd
    ,fec_fin
    ,aud_user
    ,aud_tim
    ,cod_regid
    ,cod_audit
    ,year
    ,month
    ,day
FROM {rd_informacional}.tenjcrct WHERE year=$1 AND month=$2 AND day=$3 AND last_version=1