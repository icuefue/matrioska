INSERT  INTO TABLE {md_eva}.producto PARTITION (year=$1, month=$2, day=$3)
select distinct
cod_area_i as ultimoAnio,
cod_areapro as producto,
cod_progl as subproducto,
des_progl as descripcionProducto
from {rd_informacional}.tenwfduc
where year=$1 AND month=$2 AND day=$3 AND last_version=1