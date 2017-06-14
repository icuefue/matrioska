CREATE OR REPLACE VIEW {md_netcash_analytics}.servicios_contratados
(
	fec_proceso COMMENT 'Fecha datos'
	,cod_contrato COMMENT 'Contrato Telematico-Folio-Dependiente'
	,cod_refex COMMENT 'Referencia externa'
	,cod_canal COMMENT 'Canal'
	,cod_producto COMMENT 'Producto'
	,cod_subprodu COMMENT 'Subproducto'
	,cod_unidad COMMENT 'Unidad (banco oficina)'
	,cod_cliente COMMENT 'Identificador del titular del contrato (Cclien)'
	,cod_docum25 COMMENT 'NIF del titular del contrato telematico'
	,fec_apertura COMMENT 'Fecha Apertura del servicio'
	,fec_cancel COMMENT 'Fecha Cancelación del servicio'
	,cod_situacio COMMENT 'Estado'
	,cod_servicio COMMENT 'Codigo de servicio'
	,cod_asunto COMMENT 'Asunto del servicio en formato EDITADO'
	,xti_envirece COMMENT 'Tipo formato'
	,cod_formfich COMMENT 'Formato fichero contratado'
	,cod_clieasu COMMENT 'Identificador del titular del asunto (Cclien)'
	,cod_docuasu COMMENT 'NIF del titular del asunto'
	,des_nombre COMMENT 'Nombre y apellidos Primer Titular del asunto'
	,imp_comidis COMMENT 'Comision disponibilidad del servicio'
	,imp_comiuso COMMENT 'Comision de uso de servicio'
	,year COMMENT 'Campo particion YYYYY que indica la fecha de ingesta del fichero correspondiente al anio'
	,month COMMENT 'Campo particion MM que indica la fecha de ingesta del fichero correspondiente al mes'
	,day COMMENT 'Campo particion DD que indica la fecha de ingesta del fichero correspondiente al dia'
)
COMMENT 'Relación de Servicios Contratados para una Referencia Externa (SSTT02)'
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
,fec_apertura
,fec_cancel
,cod_situacio
,cod_servicio
,cod_asunto
,xti_envirece
,cod_formfich
,cod_clieasu
,cod_docuasu
,des_nombre
,cast(regexp_replace(trim(imp_comidis),',','.') as decimal(10,2)) as imp_comidis
,cast(regexp_replace(trim(imp_comiuso),',','.') as decimal(10,2)) as imp_comiuso
,year
,month
,day
FROM {rd_sstt}.t_servicios_contratados
--    WHERE year=$1 and month=$2 and day=$3