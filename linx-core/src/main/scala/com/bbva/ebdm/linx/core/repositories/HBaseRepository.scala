package com.bbva.ebdm.linx.core.repositories

/**
  * Acciones sobre una base de datos  HBase
  *
  * @author xe54068
  */
trait HBaseRepository {

  /**
    * Inserción de un valor en una tabla de HBase
    *
    * @param schema - esquema donde está la tabla
    * @param table - tabla donde se quiere insertar
    * @param value - valor a insertar
    */
  def write(schema: String, table: String, value: (String, Map[String, Map[String, String]]))

}
