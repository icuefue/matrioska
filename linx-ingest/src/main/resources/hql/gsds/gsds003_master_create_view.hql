CREATE OR REPLACE VIEW {md_gsds}.t_rfq_ret
(
  gid COMMENT 'ID de la operación',
  branch COMMENT 'Identifica el área dentro de una región desde la que se solicita la petición: Londres, París, Madrid, etc.',
  client COMMENT 'Cliente que solicita la operación',
  platform COMMENT 'Plataforma desde la que ha entrado la operación',
  dealtype COMMENT 'Tipo de la petición: SPOT, SWAP ,FOWARD, MM, NDF etc',
  nearleg COMMENT 'Fecha liquidación de la pata corta expresado en texto, TOD, TOM,1WEEK',
  farleg COMMENT 'Fecha liquidación de la pata larga expresado en texto',
  currencypair COMMENT 'Par de monedas que intervienen en la petición',
  dealside COMMENT 'Direccion. Compra o venta',
  amount COMMENT 'Cantidad solicitada',
  amountcurrency COMMENT 'Moneda solicitada',
  riskamount COMMENT 'Cantidad expresada en USD',
  tickettype COMMENT 'Tipo del ticket. Manual o Oneclick',
  competition COMMENT 'Operación en competición. Si o No',
  valuedate COMMENT 'Fecha de liquidación de la pata 0',
  farvaluedate COMMENT 'Fecha de liquidación de la pata 1',
  submitid COMMENT 'Identificador del envio',
  clientgroup_name COMMENT 'Nombre del grupo del cliente',
  clientgroup_type COMMENT 'Tipo del grupo del cliente',
  dealway COMMENT 'Way1/WAY2. Indica cómo se agrede al precio',
  account COMMENT 'Cuenta del cliente',
  trader COMMENT 'Identificador del trader',
  instrument COMMENT 'Tipo de instrumento. FX Cross, FX Deposit o MM Deposit',
  lastevent_status COMMENT 'Estado final de la operación',
  highlevelstatus COMMENT 'Clasificación del estado final de la operación (Cancelled / Completed / Dealer_Intervention / Desks_Closed / Quoted / Rejected)',
  quoted COMMENT 'Marca si se respondió con precio (Yes/No/Unk)',
  dispersion COMMENT 'Si l operación tiene instrucciones de liquidación especiales (Si/No)',
  requirements COMMENT 'Relativo a las instrucciones de liquidación especiales',
  ordertype COMMENT 'Tipo de orden' ,
  addinfo COMMENT 'Información adicional',
  lastupdate COMMENT 'Fecha de la última actualización de la RFQ',
  anio COMMENT 'YYYY Año de ingesta del fichero',
  mes COMMENT 'MM Mes de ingesta del fichero',
  dia COMMENT 'DD Día de ingesta del fichero'
)
COMMENT 'GSDS 003.Planificación de la extracción diaria de las órdenes de FX (realizadas en RET), que son almacenados diariamente en la intranet por la aplicación SIMON'
AS
select * 
  from {rd_gsds}.rfq_ret_c
union all
select * 
  from {rd_gsds}.rfq_ret
 where anio>$1
    or (anio=$1 and mes>$2)
    or (anio=$1 and mes=$2 and dia>$3)