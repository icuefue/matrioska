CREATE OR REPLACE VIEW {md_tablasgenerales}.participe_grupo_tfm
(
    cod_cclien COMMENT 'Codigo de cliente de Clientela. [[17878]]'
    ,cod_grupogi COMMENT 'Codigo de Grupo Empresarial. [[17886]]'
    ,fec_ini COMMENT 'Fecha de inicio de vigencia del dato [[30]]'
    ,cod_entalfa COMMENT 'CODIGO ISO DE LA ENTIDAD.SE TRATA DEL CODIGO UNIVERSAL DE LA ENTIDAD Ejemplo. 0182 BBVA. S.A.'
    ,cod_cifgi COMMENT 'Codigo de identificacion fiscal de cliente (CIF, NIF, Pasaporte, etc). [[17924]]'
    ,des_nombregi COMMENT 'Razon social o nombre y apellidos del cliente. [[5647]]'
    ,des_direccgi COMMENT 'Direccion. [[17918]]'
    ,des_poblacgi COMMENT 'Poblacion. [[17955]]'
    ,des_provingi COMMENT 'Provincia. [[17959]]'
    ,cod_codposgi COMMENT 'Codigo postal. [[17896]]'
    ,cod_paisoalf COMMENT 'CODIGO DE PAIS ISO ALFABETICO. ES EL CODIGO ALFABETICO SEGUN NORMAS ISO. [[17894]]'
    ,xti_cabetfm COMMENT 'Indicador de cliente cabecera de grupo TFM. [[17929]]'
    ,fec_fin COMMENT 'Fecha de fin de vigencia del dato [[17994]]'
    ,cod_ccliept COMMENT 'Codigo de cliente de Clientela. Si la persona es fisica, se informa el valor asociado al codigo de persona fisica generico (999999999) [[17997]]'
    ,aud_user COMMENT 'Auditoria usuario'
    ,aud_tim COMMENT 'Auditoria timestamp'
    ,cod_regid COMMENT 'Codigo de registro'
    ,cod_audit COMMENT 'Codigo de auditoria'
    ,year COMMENT 'year'
    ,month COMMENT 'month'
    ,day COMMENT 'day'
) COMMENT 'Esta tabla contiene las relaciones con clientes de los grupos de la aplicacion TFM correspondiente a CRM Plus.'
AS
SELECT
    cod_cclien
    ,cod_grupogi
    ,fec_ini
    ,cod_entalfa
    ,cod_cifgi
    ,des_nombregi
    ,des_direccgi
    ,des_poblacgi
    ,des_provingi
    ,cod_codposgi
    ,cod_paisoalf
    ,xti_cabetfm
    ,fec_fin
    ,cod_ccliept
    ,aud_user
    ,aud_tim
    ,cod_regid
    ,cod_audit
    ,year
    ,month
    ,day
FROM {rd_informacional}.tenjctdf WHERE year=$1 AND month=$2 AND day=$3 AND last_version=1