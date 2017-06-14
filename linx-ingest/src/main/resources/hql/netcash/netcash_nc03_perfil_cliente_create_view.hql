CREATE OR REPLACE VIEW {md_netcash_analytics}.perfil_cliente
(
	fec_process COMMENT 'Fecha datos'
    ,cod_reference COMMENT 'Referencia'
    ,cod_user COMMENT 'Usuario'
    ,qty_signedorders COMMENT 'Número de órdenes firmadas'
    ,fec_lastsignature COMMENT 'Fecha última firma'
    ,qty_smssent COMMENT 'Número de SMSs enviados en los ultimos 3 meses'
    ,fec_lastsmssent COMMENT 'Fecha último SMS enviado'
    ,qty_daysmssent COMMENT 'Número de SMSs enviados en el ultimo dia'
    ,year COMMENT 'Campo particion YYYYY que indica la fecha de ingesta del fichero correspondiente al anio'
    ,month COMMENT 'Campo particion MM que indica la fecha de ingesta del fichero correspondiente al mes'
    ,day COMMENT 'Campo particion DD que indica la fecha de ingesta del fichero correspondiente al dia'
)
COMMENT 'PerfilCliente - RefExterna'
AS 
SELECT
fec_process
,cod_reference
,cod_user
,cast(qty_signedorders as decimal) as qty_signedorders
,fec_lastsignature
,cast(qty_smssent as decimal) as qty_smssent
,fec_lastsmssent
,qty_daysmssent
,year
,month
,day
FROM {rd_netcash}.t_perfil_cliente
--    WHERE year=$1 and month=$2 and day=$3