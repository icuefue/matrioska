select max(from_unixtime(unix_timestamp(lastupdate, 'yyyy-MM-dd hh:mm:ss'),'yyyyMMdd'))
  from {rd_gsds}.rfq_ret_c