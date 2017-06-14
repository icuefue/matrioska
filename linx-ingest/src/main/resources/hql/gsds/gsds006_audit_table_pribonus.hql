INSERT INTO TABLE {rd_ebdmau}.t_audit_master PARTITION (FECHAPROCESO=$1) 
  SELECT '$2',
         '$3' as des_uuid, 
		 '$4', 
		 '$5',
		 '$6',
		 'master_data',
		 'master',
		 'D',
		 'tvfi_trns_mabobl_sentry',
		 '',
		 't_pri_bonus',
		 TMP.cuenta,
		 '$7',
		 '$8' as ejecutable,
		 'Carga Master Data GSDS 0006 - Tabla t_pri_bonus' 
    FROM 
	    (select count(*) as cuenta 
		   from {md_gsds}.t_pri_bonus
		  WHERE fechacarga=$1) TMP