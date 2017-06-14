INSERT INTO TABLE {md_eva}.parametro PARTITION (year=$1, month=$2, day=$3)
SELECT
cod_area as producto,
cod_desgsede as sede,
des_tippar as parametro,
fec_fecha as fecha,
por_tipparm as valor,
DAY(fec_fecha) as dia,
MONTH(fec_fecha) as mes,
YEAR(fec_fecha) as anio
FROM {rd_informacional}.TENWFKEC
where year=$1 AND month=$2 AND day=$3 AND last_version=1
union all
SELECT
'' as producto,
cod_desgsede as sede,
des_tippar as parametro,
fec_fecha as fecha,
imp_valorrep as valor,
DAY(fec_fecha) as dia,
MONTH(fec_fecha) as mes,
YEAR(fec_fecha) as anio
from {rd_informacional}.TENWFPEC
where year=$1 AND month=$2 AND day=$3 AND last_version=1
union all
SELECT cod_area as producto,
des_paisgeo as sede,
'Por Costes EUR' as parametro,
fec_fecha as fecha,
por_costes as valor,
DAY(fec_fecha) as dia,
MONTH(fec_fecha) as mes,
YEAR(fec_fecha) as anio
from {rd_informacional}.TENWFPCO
where year=$1 AND month=$2 AND day=$3 AND last_version=1