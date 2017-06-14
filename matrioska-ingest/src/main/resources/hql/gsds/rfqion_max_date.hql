select lpad(anio,4,'0') anio, lpad(mes,2,'0') mes, lpad(dia, 2, '0') dia from {rd_gsds}.rfq_ion order by anio desc, mes desc, dia desc limit 1
