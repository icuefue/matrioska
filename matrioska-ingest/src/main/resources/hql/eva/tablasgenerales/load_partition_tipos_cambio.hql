INSERT INTO TABLE {md_tablasgenerales}.tipos_cambio PARTITION (year=$1, month=$2, day=$3)
SELECT
fec_ffecha, cod_ccotiza, cod_cdisoalf, cod_cdisoalt, des_ndivi, des_nomdives, des_nomdivin,
imp_fixm, imp_fixma, imp_ffixing,xti_calenda, imp_fixingca, year, month, day
FROM {rd_informacional}.ventgtcm1
WHERE year=$1 AND month=$2 AND day=$3 AND last_version=1