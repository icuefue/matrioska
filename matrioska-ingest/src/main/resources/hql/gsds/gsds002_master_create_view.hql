CREATE OR REPLACE VIEW {md_gsds}.t_rfq_ion
(
id COMMENT 'Código identificativo de la operación en ION Trading',
marketid COMMENT 'Mercado',
actionstr COMMENT 'Última acción realizada sobre la operación',
anctstier COMMENT 'Tier del precio del instrumento',
aqquoteowner COMMENT 'Trader que está cotizando el bono',
code COMMENT 'Código del instrumento',
codetypestr COMMENT 'Tipo de código del instrumento',
currencystr COMMENT 'Moneda en la que se realiza la operación',
custfirm COMMENT 'Nombre de la contrapartida que recibimos del mercado',
custusername COMMENT 'Nombre del representante de la contrapartida',
dealertrader COMMENT 'Código del trader involucrado en la operación. este campo puede estar vacío',
dealertradername COMMENT 'Nombre del trader involucrado en la operación. este campo puede estar vacío',
descripcion COMMENT 'Descripción del instrumento',
maturitydate COMMENT 'Fecha de vencimiento del instrumento de la segunda pata de la operación',
rfqcoverprice COMMENT 'Cuando cerramos un trade. el segundo mejor precio',
rfqcreatedate COMMENT 'Fecha de creación de la operación',
rfqnlegs COMMENT 'Número de instrumentos utilizados',
rfqordertypestr COMMENT 'Inquiry u order',
rfqprice COMMENT 'Precio',
rfqqty COMMENT 'Cantidad',
rfqtypestr COMMENT 'Tipo de RFQ',
rfqverbstr COMMENT 'Dirección. Buy/Sell',
rfqyield COMMENT 'Rendimiento',
salesperson COMMENT 'Nombre del ventas del banco asociado a la operación. se obtiene de cartelización utilizando el libro del trader y la contrapartida',
salespersonid COMMENT 'Código del ventas del banco asociado a la operación. se obtiene de cartelización utilizando el libro del trader y la contrapartida',
salespersonroom COMMENT 'Grupo del ventas asociado a la operación',
statusstr COMMENT 'Estado final de la operación',
tradeid COMMENT 'Código identificativo del trade. solo tiene valor cuando es una operación que se ha realizado',
ask0 COMMENT 'Mejor precio ask en el virtual market cuando se ha ejecutado la operación. solo tiene valor cuando es una operación que se ha realizado',
bid0 COMMENT 'Mejor precio bid en el virtual market cuando se ha ejecutado la operación. solo tiene valor cuando es una operación que se ha realizado',
cpaccountid COMMENT 'Código de la contrapartida proveniente del mercado',
cpdescription COMMENT 'Descripción de la contrapartida proveniente del mercado',
country COMMENT 'País del bono de la operación',
countryisocode COMMENT 'ISO Code del país del bono de la operación',
customstring5 COMMENT 'Indicador de que grupo de traders gestionan el instrumento',
ionbook COMMENT 'Libro del trader en ION',
sensitivity COMMENT 'Sensibilidad del intrumento',
franchise COMMENT 'Franquicia',
quoted COMMENT 'Si BBVA ha devuelto precio o no',
anctscustgroup COMMENT 'Clasificacion de la contrapartida (gold, silver, bronze)',
anruleid COMMENT 'Identificador de la regla de auto negociación que cumplía la condiciones',
rfqdatesettl COMMENT 'Fecha de liquidación de la operación',
rfqdayssettl COMMENT 'Fecha de liquidación de la operación expresada en días',
rfqdealstddatesettl COMMENT 'Fecha estándar de liquidación para el instrumento utilizado',
rfqdealstddayssettl COMMENT 'Fecha estándar de liquidación para el instrumento utilizado expresado en días',
exchangerate COMMENT 'Tipo de cambio',
firmprice COMMENT 'Cotizado en firme. Solo aplica a bloomberg',
nleg COMMENT 'Número de patas de la operación',
stptargetid COMMENT 'Código star de la operación',
stptargetstatus COMMENT 'Estado de la operación en star. Recibida, Rechazada, En Espera',
codcnae COMMENT 'Actividad económica de la contrapartida',
desccnae COMMENT 'Descripción de la actividad económica de la contrapartida',
ratingbbgcomp COMMENT 'Rating BBG COMP',
ratingfitch COMMENT 'Rating FITCH',
ratingmoodys COMMENT 'Rating MOODYS',
ratingstandarpoors COMMENT 'Rating STANDAR & POORS',
issuesizeoutstanding COMMENT 'Cantidad emitida',
anio COMMENT 'YYYY Año de ingesta del fichero',
mes COMMENT 'MM Mes de ingesta del fichero',
dia COMMENT 'DD Día de ingesta del fichero'
)
COMMENT 'GSDS 001. Planificación de la extracción diaria de las operaciones de ION trading de los mercados de distribución (BLOOMBERG, TRADEWEB y BONDVISION) de las mesas de crédito y de gobiernos. Estos datos están actualmente almacenados en la intranet.'
AS
select * 
  from {rd_gsds}.rfq_ion_c
union all
select * 
  from {rd_gsds}.rfq_ion
 where anio>$1
    or (anio=$1 and mes>$2)
    or (anio=$1 and mes=$2 and dia>$3)