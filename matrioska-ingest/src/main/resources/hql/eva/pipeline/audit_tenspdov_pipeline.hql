INSERT INTO TABLE {rd_ebdmau}.t_audit_master PARTITION (fechaproceso=$1)
SELECT '$5','$6' AS des_uuid, '$7', '$8',
'PIPELINE(V_TENSPDOV)','master_data','Base de Datos','D','Varias (Insert ... Select)','',
'PIPELINE(V_TENSPDOV)',tmp.cuenta,'$9','$10' AS ejecutable,
'Carga Master Data PIPELINE - Vista V_TENSPDOV' FROM (SELECT COUNT(*) AS cuenta FROM {md_eva}.v_tenspdov) tmp