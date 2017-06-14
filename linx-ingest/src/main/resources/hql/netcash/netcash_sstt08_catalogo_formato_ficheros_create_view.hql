CREATE OR REPLACE VIEW {md_netcash_analytics}.catalogo_formato_ficheros
(
    fec_proceso COMMENT 'Fecha datos(YYYY-MM-DD)'
    ,cod_formfich COMMENT 'Codigo de formato de fichero'
    ,des_formfich COMMENT 'Descripcion de formato de fichero'
    ,year COMMENT 'Campo particion YYYYY que indica la fecha de ingesta del fichero correspondiente al anio'
    ,month COMMENT 'Campo particion MM que indica la fecha de ingesta del fichero correspondiente al mes'
    ,day COMMENT 'Campo particion DD que indica la fecha de ingesta del fichero correspondiente al dia'
)
COMMENT 'Catalogo de formatos de fichero (SSTT08)'
AS SELECT
fec_proceso
,cod_formfich
,des_formfich
,year
,month
,day
FROM {rd_sstt}.t_formato_ficheros
    WHERE year=$1 and month=$2 and day=$3