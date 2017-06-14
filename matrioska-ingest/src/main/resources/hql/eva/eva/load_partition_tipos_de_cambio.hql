INSERT  INTO TABLE {md_tablasgenerales}.tiposdecambio PARTITION (year=$1, month=$2, day=$3)
select distinct cast(substring(fec_ffecha,1,10) as date) AS FECHA, cod_cdivisa as divisa, imp_fixm as FIXINGM, imp_fixma as FIXINGMA,
DAY(cast(substring(fec_ffecha,1,10) as date)) as dia,
MONTH(cast(substring(fec_ffecha,1,10) as date)) as mes,
YEAR(cast(substring(fec_ffecha,1,10) as date)) as anio
from {rd_informacional}.tenbtctr where
cod_cdivisa in ('AED', 'ALL', 'ARS', 'AUD', 'BGN', 'BHD', 'BOB', 'BRL', 'BSD', 'BYR', 'CAD', 'CHF', 'CLF', 'CLP', 'CNY', 'COP', 'CRC', 'CZK', 'DKK', 'DOP', 'DZD', 'EEK', 'EGP', 'EUR', 'GBP', 'GIP', 'GNF', 'GTQ', 'HKD', 'HNL', 'HRK', 'HUF', 'IDR', 'ILS', 'INR', 'IQD', 'ISK', 'JOD', 'JPY', 'KRW', 'KWD', 'KYD', 'KZT', 'LTL', 'MAD', 'MXN', 'MXV', 'MYR', 'NOK', 'NZD', 'PAB', 'PEN', 'PHP', 'PLN', 'PYG', 'QAR', 'RON', 'RUB', 'SAR', 'SEK', 'SGD', 'SVC', 'THB', 'TND', 'TRY', 'TWD', 'UAH', 'USD', 'UVR', 'UYU', 'XAF', 'XAG', 'XAU', 'ZAR')
AND year=$1 AND month=$2 AND day=$3 AND last_version=1
union all
select distinct cast(substring(fec_ffecha,1,10) as date) AS FECHA, cod_cdisoalf as divisa, imp_fixm as FIXINGM, imp_fixma as FIXINGMA,
DAY(cast(substring(fec_ffecha,1,10) as date)) as dia,
MONTH(cast(substring(fec_ffecha,1,10) as date)) as mes,
YEAR(cast(substring(fec_ffecha,1,10) as date)) as anio
FROM {rd_informacional}.tentgtcm where
cod_cdisoalf = 'VEF'
AND year=$1 AND month=$2 AND day=$3 AND last_version=1