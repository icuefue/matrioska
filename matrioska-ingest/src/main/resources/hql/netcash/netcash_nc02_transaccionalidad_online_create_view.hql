CREATE OR REPLACE VIEW {md_netcash_analytics}.transaccionalidad_online 
(
    fec_process        COMMENT 'Fecha datos'
   ,cod_reference      COMMENT 'Referencia'
   ,cod_product        COMMENT 'Producto'
   ,cod_byproduct      COMMENT 'Subproducto'
   ,cod_service        COMMENT 'Servicio'
   ,fec_operation      COMMENT 'Fecha de la operacion'
   ,fec_authoperation  COMMENT 'Fecha autorización operacion'
   ,qty_ordquantity    COMMENT 'Numero de ordenes'
   ,qty_filequantity   COMMENT 'Numero de ficheros implicados'
   ,imp_operation      COMMENT 'Importe de la operacion'
   ,cod_currency       COMMENT 'Divisa de la operacion'
   ,cod_payer          COMMENT 'Ordenante de la operacion'
   ,cod_insertion      COMMENT 'Forma de incorporacion'
   ,year               COMMENT 'Campo particion YYYYY que indica la fecha de ingesta del fichero correspondiente al anio'
   ,month              COMMENT 'Campo particion MM que indica la fecha de ingesta del fichero correspondiente al mes'
   ,day                COMMENT 'Campo particion DD que indica la fecha de ingesta del fichero correspondiente al dia'
)
COMMENT 'UsoServicios - RefExterna'
AS
SELECT  fec_process
       ,cod_reference
       ,cod_product
       ,cod_byproduct
       ,cod_service
       ,fec_operation
       ,fec_authoperation
       ,cast(qty_ordquantity as decimal) as qty_ordquantity
       ,cast(qty_filequantity as decimal) as qty_filequantity
       ,cast(imp_operation as decimal(10,2)) as imp_operation
       ,cod_currency
       ,cod_payer
       ,cod_insertion
       ,year
       ,month
       ,day
  FROM {rd_netcash}.t_uso_servicios