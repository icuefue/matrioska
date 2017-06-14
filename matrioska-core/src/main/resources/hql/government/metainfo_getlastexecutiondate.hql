select from_unixtime(
          max(unix_timestamp(substr(fec_hms_timestamp_log,1,19), 'yyyy/MM/dd HH:mm:ss')), 
          'yyyy-MM-dd HH:mm:ss') max_fec
  from {rd_ebdmau}.t_log_executions
 where des_scriptbd='$1'
   and des_estado_proceso='$2'