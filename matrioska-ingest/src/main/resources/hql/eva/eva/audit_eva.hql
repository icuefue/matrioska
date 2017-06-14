INSERT INTO TABLE {rd_ebdmau}.t_audit_master PARTITION (FECHAPROCESO=$1)
SELECT '$5','$6' as des_uuid, '$7', '$8','EVA','master_data','Base de Datos','D',
'Varias (Insert ... Select)','','eva',TMP.cuenta,'$9','$10' as ejecutable,
'Carga Master Data EVA - Tabla EVA'
FROM (select count(*) as cuenta from {md_eva}.eva WHERE year=$2 AND month=$3 AND day=$4) TMP
