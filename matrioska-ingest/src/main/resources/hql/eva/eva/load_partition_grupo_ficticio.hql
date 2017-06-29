INSERT INTO TABLE {md_eva}.grupoficticio PARTITION (year=$1, month=$2, day=$3)
select
gfi.cod_grupogfi as cod_grupo,
gfi.des_grupogfi as des_grupo,
null as gestor,
gfi.des_tipol_gp as tipologia,
gfi.des_tier_gp as tier,
gfi.des_pais_gru as paisOrigen,
gfi.cod_pais_gru as cod_paisOrigen,
gfi.des_segmento as segmento,
gfi.des_subseg as subsegmento,
gfi.des_sedegest as sedeGestion,
null as cod_sedeGestion,
gfi.des_gestih as industria,
null as cod_industria,
gfi.xti_grupoace as grupoACE,
'N' as multigrupo,
null as docfis,
gfi.cod_subsgfi as cod_actividad,
gfi.des_subsgfi as actividad,
null as paisGestion,
null as cod_paisGestion,
'N' as baja,
null as fechaBaja
from (select * from {rd_informacional}.tenwfgfi where year=$1 AND month=$2 AND day=$3 AND last_version=1) gfi