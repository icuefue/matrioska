insert into table {md_tablasgenerales}.paisdivisasede PARTITION (year=$1, month=$2, day=$3)
select 
t.cod_pais,
t.pais,
t.cod_sede,
divisa.cod_divisa,
divisa.divisa,
t.sede,
t.ind_sedeCib,
t.ind_sedeBbva,
t.ind_sedeEVA
from
(
select
pais.cod_paisoalf as cod_pais,
pais.des_pais_lar as pais,
sede.cod_sedeg as cod_sede,
sede.cod_desgsede as sede,
sede.xti_sedecib as ind_sedeCib,
sede.xti_sedebbva as ind_sedeBbva,
sede.xti_sdcibeva as ind_sedeEVA
from
(select * from {rd_informacional}.tenydpai where year=$1 AND month=$2 AND day=$3 AND last_version=1) pais
left outer join (select * from {rd_informacional}.tentgges where year=$1 AND month=$2 AND day=$3 AND last_version=1) sede
on ( 
pais.cod_paisoalf = sede.cod_paisoalf
and sede.cod_paisoalf is not null 
and date(pais.fec_fin_s) = '9999-12-31'
and date(sede.fec_fin_s) = '9999-12-31'
and pais.xti_paisoint = 0 
)
)t
left join {rd_informacional}.t_pais_divisa divisa
on 
divisa.cod_pais=t.cod_pais
