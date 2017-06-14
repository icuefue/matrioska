CREATE OR REPLACE VIEW {md_netcash_analytics}.catalogo_situacion_ficheros
(
	fec_proceso COMMENT 'Fecha datos(YYYY-MM-DD)'
    ,cod_situfic COMMENT 'Codigo de  situacion'
    ,des_situfic COMMENT 'Descripcion de situacion'
    ,xsn_definit COMMENT 'Indicador de definitivo'
    ,year COMMENT 'Campo particion YYYYY que indica la fecha de ingesta del fichero correspondiente al anio'
    ,month COMMENT 'Campo particion MM que indica la fecha de ingesta del fichero correspondiente al mes'
    ,day COMMENT 'Campo particion DD que indica la fecha de ingesta del fichero correspondiente al dia'
)
COMMENT 'Catalogo de situaciones de fichero (SSTT07)'
AS SELECT
fec_proceso
,cod_situfic
,des_situfic
,xsn_definit
,year
,month
,day
FROM {rd_sstt}.t_situacion_ficheros
    WHERE year=$1 and month=$2 and day=$3