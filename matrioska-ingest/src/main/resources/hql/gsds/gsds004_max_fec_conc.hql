select max(from_unixtime(unix_timestamp(date, 'yyyy-MM-dd'),'yyyyMMdd'))
  from {rd_gsds}.trades_eurex_ion_c