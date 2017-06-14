CREATE OR REPLACE VIEW {md_netcash_analytics}.normalizador
(
	fec_proceso COMMENT 'Fecha datos'
	,cod_contrato COMMENT 'Contrato Telematico-Folio-Dependiente'
	,cod_refex COMMENT 'Referencia externa'
	,cod_canal COMMENT 'Canal'
	,cod_producto COMMENT 'Producto'
	,cod_subprodu COMMENT 'Subproducto'
	,cod_unidad COMMENT 'Unidad (banco oficina)'
	,cod_cliente COMMENT 'Identificador del titular del contrato (Cclien)'
	,cod_docum25 COMMENT 'NIF del titular del contrato telemático'
	,cod_servinor COMMENT 'Servicio de Normalizacion'
	,des_servinor COMMENT 'Descripcion del servicio de Normalizacion'
	,cod_servicio COMMENT 'Codigo del servicio normalizado'
	,des_servicio COMMENT 'Descripcion del servicio normalizado'
    ,year COMMENT 'Campo particion YYYYY que indica la fecha de ingesta del fichero correspondiente al anio'
    ,month COMMENT 'Campo particion MM que indica la fecha de ingesta del fichero correspondiente al mes'
    ,day COMMENT 'Campo particion DD que indica la fecha de ingesta del fichero correspondiente al dia'
)
COMMENT 'Relación de Servicios Normalizados contratados (SSTT03)'
AS SELECT
fec_proceso
,cod_contrato
,cod_refex
,cod_canal
,cod_producto
,cod_subprodu
,cod_unidad
,cod_cliente
,cod_docum25
,cod_servinor
,des_servinor
,cod_servicio
,des_servicio
,year
,month
,day
FROM {rd_sstt}.t_servicios_normalizados_contratados
    WHERE year=$1 and month=$2 and day=$3