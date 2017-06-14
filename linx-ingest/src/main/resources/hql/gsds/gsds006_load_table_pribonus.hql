INSERT INTO TABLE {md_gsds}.t_pri_bonus PARTITION (fechacarga=$1) 

select tName.id,tName.Name,tDtSrc.DtSrc,tPxStrctTyp.PxStrctTyp,tIssuDtTm.IssuDtTm,tLastUpdateTm.LastUpdateTm,tVal.Val as Val,tMktDataTyp.MktDataTyp
from
(
SELECT  row_sequence()id,t1.Name FROM 
(
SELECT Name
from (select xmldata from {rd_gsds}.tvfi_trns_mabobl_sentry where anio=$2 and mes=$3 and dia=$4 )txml
lateral view explode(xpath(txml.xmldata, "//*[name()='FIXML']/*[name()='ValRpt']/*[name()='Mkt']/*[name()='Ric']/*[name()='BySrc']/*[name()='Pnt']/@Name"))tName as Name
)t1
)tName
join
(
SELECT row_sequence()id,t2.DtSrc FROM 
(
SELECT DtSrc
from (select xmldata from {rd_gsds}.tvfi_trns_mabobl_sentry where anio=$2 and mes=$3 and dia=$4 )txml
lateral view explode(xpath(txml.xmldata, "//*[name()='FIXML']/*[name()='ValRpt']/*[name()='Mkt']/*[name()='Ric']/*[name()='BySrc']/*[name()='Pnt']/@DtSrc"))tDtSrc as DtSrc
)t2
)tDtSrc
on tName.id=tDtSrc.id
join
(
SELECT row_sequence()id,t3.PxStrctTyp FROM 
(
SELECT PxStrctTyp
from (select xmldata from {rd_gsds}.tvfi_trns_mabobl_sentry where anio=$2 and mes=$3 and dia=$4 )txml
lateral view explode(xpath(txml.xmldata, "//*[name()='FIXML']/*[name()='ValRpt']/*[name()='Mkt']/*[name()='Ric']/*[name()='BySrc']/*[name()='Pnt']/*[name()='Undly']/@PxStrctTyp"))tPxStrctTyp as PxStrctTyp
)t3
)tPxStrctTyp
on tName.id=tPxStrctTyp.id 
join
(
SELECT row_sequence()id,t4.IssuDtTm FROM 
(SELECT IssuDtTm
from (select xmldata from {rd_gsds}.tvfi_trns_mabobl_sentry where anio=$2 and mes=$3 and dia=$4 )txml
lateral view explode(xpath(txml.xmldata, "//*[name()='FIXML']/*[name()='ValRpt']/*[name()='Mkt']/*[name()='Ric']/*[name()='BySrc']/*[name()='Pnt']/*[name()='Undly']/@IssuDtTm"))tIssuDtTm as IssuDtTm
)t4
)tIssuDtTm
on tName.id=tIssuDtTm.id  
join
(
SELECT row_sequence()id,t5.LastUpdateTm FROM 
(SELECT LastUpdateTm
from (select xmldata from {rd_gsds}.tvfi_trns_mabobl_sentry where anio=$2 and mes=$3 and dia=$4 )txml
lateral view explode(xpath(txml.xmldata, "//*[name()='FIXML']/*[name()='ValRpt']/*[name()='Mkt']/*[name()='Ric']/*[name()='BySrc']/*[name()='Pnt']/*[name()='Undly']/@LastUpdateTm"))tLastUpdateTm as LastUpdateTm
)t5
)tLastUpdateTm
on tName.id=tLastUpdateTm.id 
join
(
SELECT row_sequence()id,t6.Val FROM 
(SELECT 
Val
from (select xmldata from {rd_gsds}.tvfi_trns_mabobl_sentry where anio=$2 and mes=$3 and dia=$4 )txml
lateral view explode(xpath(txml.xmldata, "//*[name()='FIXML']/*[name()='ValRpt']/*[name()='Mkt']/*[name()='Ric']/*[name()='BySrc']/*[name()='Pnt']/*[name()='Msr']/@Val"))tVal as Val
)t6
)tVal
on tName.id=tVal.id 
join
(
SELECT row_sequence()id, t8.MktDataTyp FROM 
(SELECT MktDataTyp
from (select xmldata from {rd_gsds}.tvfi_trns_mabobl_sentry where anio=$2 and mes=$3 and dia=$4 )txml
lateral view explode(xpath(txml.xmldata, "//*[name()='FIXML']/*[name()='ValRpt']/*[name()='Mkt']/*[name()='Ric']/*[name()='BySrc']/*[name()='Pnt']/*[name()='Msr']/@MktDataTyp"))tMktDataTyp as MktDataTyp
)t8
)tMktDataTyp
on tName.id=tMktDataTyp.id 