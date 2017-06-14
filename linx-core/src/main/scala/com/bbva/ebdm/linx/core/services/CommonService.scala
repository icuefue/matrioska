package com.bbva.ebdm.linx.core.services

import com.bbva.ebdm.linx.core.beans.RdbmsOptions
import com.bbva.ebdm.linx.core.enums.PartitionTypes.PartitionType
import org.apache.spark.sql.DataFrame

trait CommonService {

  /**
   * Realiza la importación de una tabla de un sistema relacional a la tabla hive relacionada.
   * Realiza tres intentos de importación y si no lo consigue mantiene los datos anteriores.
   * Realiza la auditoría y si no coincide el numero de registros de la tabla origen con la de destino
   * mantiene la tabla anterior.
   * 
   * @param options Objeto donde se almacena la información nesesaria 
   * sobre la tabla de origen y la tabla destino para poder realizar la importación
   */
  def importTable(options: RdbmsOptions)

  /**
    * Consulta de la máxima partición (la de fecha mayor) encontrada para una tabla particionada por year(int), month(int), day(int)
    *
    *
    * @param schema - esquema donde está la tabla
    * @param table - tabla a la que consultar su máxima partición
    *
    * @return Option[String] - máxima partición encontrada en formato YYYYMMDD
    */
  def getMaxPartition(schema: String, table: String) : Option[String]
  
    /**
    * Comprueba si un DataFrame está vacío o no
    *
    *
    * @param df - DataFrame a validar
    *
    * @return Boolean - True si está vacío, false en caso contrario
    */
  def isEmptyDF(df: DataFrame) : Boolean
}
