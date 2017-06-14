CREATE OR REPLACE VIEW {md_gsds}.t_orders_eurex_ion
(
  id COMMENT 'Identificador de la orden',
  date COMMENT 'Fecha en que se realizó la orden',
  time COMMENT 'Hora en que se realizó la orden',
  price COMMENT 'Precio pedido',
  qtygoal COMMENT 'Cantidad pedida',
  trader COMMENT 'Trader que hace la orden',
  code COMMENT 'Código del instrumento',
  orderno COMMENT 'Identificador de la orden',
  algoid COMMENT 'AlgoId enviado a Eurex',
  verbstr COMMENT 'Dirección (Compra o Venta)',
  activestr COMMENT 'Si esta activa la orden',
  member COMMENT 'BVABB',
  platform COMMENT 'ION /ORC /GL',
  idorder COMMENT 'Identificador interno',
  market COMMENT 'Mercado (CMF)',
  anio COMMENT 'YYYY Año de ingesta del fichero',
  mes COMMENT 'MM Mes de ingesta del fichero',
  dia COMMENT 'DD Día de ingesta del fichero'
)
COMMENT 'GSDS 004.Planificación de la extracción diaria de las operaciones de ION Trading de los mercados interbancarios (MTS,SENAF, EUREXBONDS Y BROKERTEC) de las mesas de crédito y de gobiernos.'
AS
select * 
  from {rd_gsds}.orders_eurex_ion_c
union all
select * 
  from {rd_gsds}.orders_eurex_ion
 where anio>$1
    or (anio=$1 and mes>$2)
    or (anio=$1 and mes=$2 and dia>$3)