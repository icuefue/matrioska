package com.bbva.ebdm.linx.ingest.services

trait CommonService {

  /**
    * Creación del flag de la carga en raw de una tabla ({schema}-{table}.flg) en la ruta cfg/flags/raw
    *
    * @param schema - Esquema
    * @param table - Tabla
    */
  def createRawFlag(schema: String, table: String)

  /**
    * Borrado del flag de la carga en raw de una tabla ({schema}-{table}.flg) en la ruta cfg/flags/raw
    *
    * @param schema - Esquema
    * @param table - Tabla
    */
  def deleteRawFlag(schema: String, table: String)

  /**
    * Importar tablas de un origen a un destino de un determinado caso de uso
    *
    * @param useCase - Caso de uso
    */
  def importTables(useCase: String)
}

