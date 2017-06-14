CREATE OR REPLACE VIEW {md_netcash_analytics}.contratos
(
	fec_proceso COMMENT 'Fecha datos(AAAA-MM-DD)'
	,cod_contrato COMMENT 'Contrato Telematico-Folio-Dependiente (contrapartida 525) formato: (CCCC-FFFFFFFFF-DDDDDD)'
	,cod_refex COMMENT 'Referencia externa'
	,cod_canal COMMENT 'Canal'
	,cod_producto COMMENT 'Producto'
	,cod_subprodu COMMENT 'Subproducto'
	,cod_unidad COMMENT 'Unidad (banco oficina)'
	,cod_cliente COMMENT 'Identificador del cliente (Cclien)'
	,cod_docum25 COMMENT 'NIF del titular del contrato telematico'
	,cod_iuc COMMENT 'Identificador del contrato (IUC)'
	,fec_apertura COMMENT 'Fecha Apertura del contrato telematico'
	,fec_cancel COMMENT 'Fecha Cancelacion del contrato telematico'
	,cod_situacio COMMENT 'Estado (Activado - 008, Pdte activar - 0010, Cancelado - 0009)'
	,fec_priconex COMMENT 'Fecha Primera Utilizacion'
	,fec_ultconex COMMENT 'Fecha Ultima Conexion'
	,des_nombre COMMENT 'Nombre Y Apellidos del titular del contrato telematico'
	,xti_tipopers COMMENT 'Tipo de persona: F - Fisica, J - Juridica'
	,xti_segmento COMMENT 'Segmento'
	,cod_tipovia COMMENT 'Tipo via del titular del contrato telematico'
	,des_nomvia COMMENT 'Via del titular del contrato telematico'
	,cod_numvia COMMENT 'Numero via del titular del contrato telematico'
	,des_poblacio COMMENT 'Poblacion del titular del contrato telematico'
	,des_provinc COMMENT 'Provincia del titular del contrato telematico'
	,cod_cpostal COMMENT 'Codigo Postal del titular del contrato telematico'
	,imp_comiaper COMMENT 'Comision de Apertura ABC2006-109'
	,imp_comimant COMMENT 'Comision de Mantenimiento ABC2002-109'
	,imp_usotr1 COMMENT 'Comision recepcion - Tramo1 (de 7:30h a 16:30h) ABC2085'
	,imp_usotr2 COMMENT 'Comision recepcion - Tramo2 (de 16:30h a 22h)'
	,imp_usotr3 COMMENT 'Comision recepcion - Tramo3 (de 22h a 7:30h)'
	,cod_ctacargo COMMENT 'Cuenta de cargo de comisiones asociada (ESXXBBBBOOOODDFFFFFFFFFC)'
	,xti_firma COMMENT 'Tipo de firma'
	,xti_admon COMMENT 'Tipo de administracion (Monousuario/Estandar/Avanzada)'
	,year COMMENT 'Campo particion YYYYY que indica la fecha de ingesta del fichero correspondiente al anio'
	,month COMMENT 'Campo particion MM que indica la fecha de ingesta del fichero correspondiente al mes'
	,day COMMENT 'Campo particion DD que indica la fecha de ingesta del fichero correspondiente al dia'
)
COMMENT 'Asociacion de una Referencia Externa a un Cliente (SSTT01)'
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
,cod_iuc
,fec_apertura
,fec_cancel
,cod_situacio
,fec_priconex
,fec_ultconex
,des_nombre
,xti_tipopers
,xti_segmento
,cod_tipovia
,des_nomvia
,cod_numvia
,des_poblacio
,des_provinc
,cod_cpostal
,cast(regexp_replace(trim(imp_comiaper),',','.') as decimal(10,2)) as imp_comiaper
,cast(regexp_replace(trim(imp_comimant),',','.') as decimal(10,2)) as imp_comimant
,cast(regexp_replace(trim(imp_usotr1),',','.') as decimal(10,2)) as imp_usotr1
,cast(regexp_replace(trim(imp_usotr2),',','.') as decimal(10,2)) as imp_usotr2
,cast(regexp_replace(trim(imp_usotr3),',','.') as decimal(10,2)) as imp_usotr3
,cod_ctacargo
,xti_firma
,xti_admon
,year
,month
,day
FROM {rd_sstt}.t_referencia_externa_cliente
    WHERE year=$1 and month=$2 and day=$3