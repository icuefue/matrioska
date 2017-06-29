INSERT INTO TABLE {rd_ebdmau}.t_audit_master PARTITION (FECHAPROCESO=$1)
SELECT
'$5' AS fecha,
'$6' AS des_uuid,
'$7',
'$8',
'INFORMACIONAL' AS caso_uso,
'MASTER DATA' AS proceso,
'BBDD' AS tipo_carga,
'D' AS frecuencia,
'rd_informacional.tenydgvc' AS nombre_objeto_origen,
tmp.cuenta_raw AS nro_reg_obj_origen,
'md_tablas_generales.clientes_int' AS nombre_obj_destino,
tmp2.cuenta_master AS nro_reg_obj_destino,
'$9' AS usuario,
'$10' AS ejecutable,
'Auditoria de MASTER DATA CLIENTES INTERNACIONALES' AS descripcion
FROM
(SELECT COUNT(*) AS cuenta_raw FROM {rd_informacional}.tenydgvc WHERE year=$2 AND month=$3 AND day=$4) tmp,
(SELECT COUNT(*) AS cuenta_master FROM {md_tablasgenerales}.clientes_int WHERE year=$2 AND month=$3 AND day=$4) tmp2