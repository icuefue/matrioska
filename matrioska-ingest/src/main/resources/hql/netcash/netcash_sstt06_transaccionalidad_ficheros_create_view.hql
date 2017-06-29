CREATE OR REPLACE VIEW {md_netcash_analytics}.transaccionalidad_ficheros (
  fec_proceso COMMENT 'Fecha datos', 
  cod_contrato COMMENT 'Contrato Telematico-Folio-Dependiente', 
  cod_refex COMMENT 'Referencia externa', 
  cod_canal COMMENT 'Canal', 
  cod_producto COMMENT 'Producto', 
  cod_subprodu COMMENT 'Subproducto', 
  cod_unidad COMMENT 'Unidad (banco oficina)', 
  cod_cliente COMMENT 'Identificador del titular del contrato (Cclien)', 
  cod_docum25 COMMENT 'NIF del titular del contrato telematico', 
  cod_servicio COMMENT 'Servicio', 
  cod_subserv COMMENT 'Subservicio (proceso)', 
  tim_operac COMMENT 'Fecha y hora ejecución operación', 
  tim_autoriz COMMENT 'Fecha y hora autorizacion operación', 
  num_ordenes COMMENT 'Número de ordenes', 
  num_fichero COMMENT 'Número de ficheros implicados', 
  tim_recep COMMENT 'Fecha y hora recepcion ficheros', 
  imp_operac COMMENT 'Importe de la operación', 
  cod_divisa COMMENT 'Divisa de la operación', 
  cod_nomord COMMENT 'Nombre Ordenante de la operación', 
  cod_ctacarg COMMENT 'Cuenta de cargo de la operación', 
  cod_situfic COMMENT 'Situacion', 
  year COMMENT 'Campo particion YYYYY que indica la fecha de ingesta del fichero correspondiente al anio', 
  month COMMENT 'Campo particion MM que indica la fecha de ingesta del fichero correspondiente al mes', 
  day COMMENT 'Campo particion DD que indica la fecha de ingesta del fichero correspondiente al dia')
COMMENT 'Uso ficheros por referencia (SSTT06)'
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
,cod_servicio
,cod_subserv
,tim_operac
,tim_autoriz
,cast(regexp_replace(trim(num_ordenes),',','.') as decimal(10)) as num_ordenes
,cast(regexp_replace(trim(num_fichero),',','.') as decimal(10)) as num_fichero
,tim_recep
,cast(regexp_replace(trim(imp_operac),',','.') as decimal(10,2)) as imp_operac
,cod_divisa
,cod_nomord
,cod_ctacarg
,cod_situfic
,year
,month
,day
FROM {rd_sstt}.t_uso_ficheros