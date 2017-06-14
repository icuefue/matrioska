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
'rd_informacional.tenorgru' AS nombre_objeto_origen,
tmp.cuenta_raw AS nro_reg_obj_origen,
'md_tablas_generales.oficinas' AS nombre_obj_destino,
tmp2.cuenta_master AS nro_reg_obj_destino,
'$9' AS usuario,
'$10' AS ejecutable,
'Auditoria de MASTER DATA OFICINAS' AS descripcion
FROM
(SELECT COUNT(*) AS cuenta_raw FROM {rd_informacional}.tenorgru WHERE year=$2 AND month=$3 AND day=$4) tmp,
(SELECT COUNT(*) AS cuenta_master FROM {md_tablasgenerales}.oficinas WHERE year=$2 AND month=$3 AND day=$4) tmp2