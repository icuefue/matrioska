CREATE OR REPLACE VIEW {md_netcash_analytics}.catalogo_servicios
(
	fec_proceso COMMENT 'Fecha datos(YYYY-MM-DD)'
	,cod_servicio COMMENT 'Servicio'
	,cod_subserv COMMENT 'Subservicio'
	,des_servicio COMMENT 'Descripcion del servicio'
	,xsn_orden COMMENT 'Orden'
	,xti_sentido COMMENT 'Sentido'
	,xti_tiposerv COMMENT 'Tipo de servicio (Operativa / Consulta / Recibidos / Enviados / Sin definir) (Indicador de Orden/Indicador de sentido)'
    ,year COMMENT 'Campo particion YYYYY que indica la fecha de ingesta del fichero correspondiente al anio'
    ,month COMMENT 'Campo particion MM que indica la fecha de ingesta del fichero correspondiente al mes'
    ,day COMMENT 'Campo particion DD que indica la fecha de ingesta del fichero correspondiente al dia'
)
COMMENT 'Catalogo de Servicios Telematicos (SSTT05)'
AS SELECT
fec_proceso
,cod_servicio
,cod_subserv
,des_servicio
,xsn_orden
,xti_sentido
,xti_tiposerv
,year
,month
,day
FROM {rd_sstt}.t_catalogo_servicios
    WHERE year=$1 and month=$2 and day=$3