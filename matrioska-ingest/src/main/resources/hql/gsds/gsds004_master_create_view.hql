CREATE OR REPLACE VIEW {md_gsds}.t_trades_eurex_ion
(
  id      COMMENT 'Identificador de la operación de ION y ORC, null para GL',
  date    COMMENT 'Fecha en que se realizó el trade',
  time    COMMENT 'Hora en que se realizó el trade',
  price   COMMENT 'Precio cerrado',
  qty     COMMENT 'Cantidad ejecutada',
  trader  COMMENT 'Trader',
  code    COMMENT 'Código del instrumento',
  orderno COMMENT 'Código de la orden asociada a este trade',
  tradenostr      COMMENT 'Identificador de la operacion',
  verbstr COMMENT 'Dirección (Compra o Venta)',
  platform        COMMENT 'ION /ORC/ GL',
  tradenostr2     COMMENT 'Identificador de las operaciones de GL, null para ION y ORC',
  tradeid COMMENT 'Identificador de las operaciones de GL, null para ION y ORC',
  idtrade COMMENT 'Identificador interno',
  market  COMMENT 'Mercado (CMF / EUREXBONDS / SENAF_REPO / SENAF)',
  anio COMMENT 'YYYY Año de ingesta del fichero',
  mes COMMENT 'MM Mes de ingesta del fichero',
  dia COMMENT 'DD Día de ingesta del fichero'
)
COMMENT 'GSDS 002.Planificación de la extracción diaria de las operaciones de ION trading de los mercados interbancarios (MTS, SENAF, EUREXBONDS y BROKERTEC) de las mesas de crédito y de gobiernos'
AS
select * 
  from {rd_gsds}.trades_eurex_ion_c
union all
select *
  from {rd_gsds}.trades_eurex_ion
 where anio>$1
    or (anio=$1 and mes>$2)
    or (anio=$1 and mes=$2 and dia>$3)