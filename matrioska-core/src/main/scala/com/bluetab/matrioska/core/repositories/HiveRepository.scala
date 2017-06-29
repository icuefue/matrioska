package com.bluetab.matrioska.core.repositories

import com.bluetab.matrioska.core.enums.CompressionCodecs.CompressionCodec
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}


/**
 * Acciones sobre un data warehouse de Hive
 *
 * @author xe54068
 *
 */
trait HiveRepository {

  /**
   * Devuelve el esquema fisico partiendo de un esquema logico(fichero YML)
   *
   * @param schema - esquema lógico de una tabla
   * @return - Option[String] devuelve el esquema de una tabla si existe
   */
  def getSchema(schema: String): Option[String]

  /**
   * Ejecución de un hql
   *
   * @param path - path al hql
   * @param args - argumentos para el hql
   * @return - DataFrame con el resultado del hql
   */
  def sql(path: String, args: String*): Option[DataFrame]

  /**
   * Devuelve la tabla especificada como un DataFrame.
   *
   * @param schema - esquema
   * @param table - tabla
   * @return - DataFrame con el contenido de la tabla
   */
  def table(schema: String, table: String): DataFrame

  /**
   * Creación de un DataFrame a partir de un RDD
   *
   * @param rdd - RDD a convertir
   * @param struct - estructura del rdd
   * @return
   */
  def createDataFrame(rdd: RDD[Row], struct: StructType): DataFrame

  /**
   * Establece un formato de compresión que se utiliza a la hora de guardar un dataframe
   *
   * @param compressionCodec - RDD a convertir
   */
  def setCompressionCodec(compressionCodec: Option[CompressionCodec])
  
   /**
   * Refresca metadata de una tabla Hive cargada previamente.
   * Necesario si se ha modificado la tabla desde otra herramienta: beeline, HDFS... 
   *
   * @param table - Nombre de la tabla a refrescar
   * @return
   */
  def refreshTable(schemaName: String, tableName: String): Unit
  
   /**
   * Escribe un Dataframe en una tabla Hive particionada
   *
   * @param DFToWrite - Dataframe a salvar en Hive
   * @param tgtSchema - Esquema de la tabla donde salvar
   * @param tgtTable - Tabla donde salvar
   * @param partition - Colección de campos con el valor del particionamiento
   * @return
   */
  def saveToTable(DFToWrite: DataFrame, tgtSchema: String, tgtTable: String, partition: Seq[String]): Unit
  
   /**
   * Escribe un Dataframe en una tabla Hive no particionada
   *
   * @param DFToWrite - Dataframe a salvar en Hive
   * @param tgtSchema - Esquema de la tabla donde salvar
   * @param tgtTable - Tabla donde salvar
   * @return
   */
  def saveToTable(DFToWrite: DataFrame, tgtSchema: String, tgtTable: String): Unit

}
