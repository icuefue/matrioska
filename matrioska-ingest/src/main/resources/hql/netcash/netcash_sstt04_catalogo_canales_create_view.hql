CREATE OR REPLACE VIEW {md_netcash_analytics}.catalogo_canales
(
	fec_proceso COMMENT 'Fecha datos(YYYY-MM-DD)'
	,cod_canal COMMENT 'Canal'
	,des_canal COMMENT 'Descripcion de Canal'
	,cod_banco COMMENT 'Codigo de banco (solo extraemos datos vigentes)'
	,cod_producto COMMENT 'Codigo de producto (solo extraemos datos vigentes)'
	,cod_subprodu COMMENT 'Codigo de subproducto (solo extraemos datos vigentes)'
	,des_producto COMMENT 'Descripcion del producto (solo extraemos datos vigentes)'
	,cod_servicio COMMENT 'Codigo de servicio (solo extraemos datos vigentes)'
    ,year COMMENT 'Campo particion YYYYY que indica la fecha de ingesta del fichero correspondiente al anio'
    ,month COMMENT 'Campo particion MM que indica la fecha de ingesta del fichero correspondiente al mes'
    ,day COMMENT 'Campo particion DD que indica la fecha de ingesta del fichero correspondiente al dia'
)
COMMENT 'Catalogo de Productos Telematicos (filtrado para banco 0182) (SSTT04)'
AS SELECT
fec_proceso
,cod_canal
,des_canal
,cod_banco
,cod_producto
,cod_subprodu
,des_producto
,cod_servicio
,year
,month
,day
FROM {rd_sstt}.t_catalogo_productos
    WHERE year=$1 and month=$2 and day=$3