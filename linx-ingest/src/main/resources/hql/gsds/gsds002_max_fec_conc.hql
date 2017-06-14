select max(from_unixtime(unix_timestamp(rfqcreatedate, 'yyyy-MM-dd'),'yyyyMMdd'))
  from {rd_gsds}.rfq_ion_c