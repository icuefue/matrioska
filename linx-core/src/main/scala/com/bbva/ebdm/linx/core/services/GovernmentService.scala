package com.bbva.ebdm.linx.core.services

import scala.collection.mutable.Map
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import com.bbva.ebdm.linx.core.beans.RdbmsOptions
import com.bbva.ebdm.linx.core.beans.UseCase
import com.bbva.ebdm.linx.core.beans.AppInfo
import com.bbva.ebdm.linx.core.enums.PartitionTypes.PartitionType


/**
  * Servicios de gobierno sobre el cluster
  */
trait GovernmentService extends Serializable {

  object LogHeaderEnum extends Enumeration {
    type LogHeaderEnum = Value
    val DETAIL, EXECUTION, AUDITRAW = Value
  }

  object LogStatusEnum extends Enumeration {
    type LogStatusEnum = Value
    val RUNNING, KO, OK = Value
  }

  /**
    * Obtención de las columnas de una tabla
    *
    * @param schema - Esquema
    * @param table - Tabla
    * @return
    */
  def getTableDefinition(schema: String, table: String): StructType

  /**
    * Obtener el número de registro de una tabla concreta
    *
    * @param schema - Esquema
    * @param table - Tabla
    * @param partitionType - Tipo de particionamiento de la tabla
    * @param planDate - Fecha de planificación (en caso de que esté particionada por fecha)
    * @return
    */
  def getTableCount(schema: String, table: String, partitionType: PartitionType, planDate: String): Long

  /**
    * Obtención de la info de la App de la tabla de planificaciones
    *
    * @param x - nombre de la App a consultar su info
    * @return Option[AppInfo] con la información encontrada
    */
  def findAppInfo(x: String): Option[AppInfo]

  /**
    *
    * @return
    */
  def findUseCases: Map[String, UseCase]

  /**
    * Inicio del log
    */
  def logOnStart

  /**
    * Finalización del log
    */
  def logOnComplete

  /**
    * Error en la app (pintarlo en el log)
    *
    * @param exception - excepción lanzada
    */
  def logOnError(exception: Throwable)

  /**
    * Auditoría de una tabla importada de un origen relacional
    *
    * @param options - Objeto RdbmsOptions usado para importar la tabla
    */
  def auditRawTable(options: RdbmsOptions)

  /**
    * Volcado de la auditoría de la tabla al log
    *
    * @param source - origen
    * @param sourceCount - conteo de registros en origen
    * @param target - destino
    * @param targetCount - conteo de registros en destino
    */
  def auditRawTable(source: String, sourceCount: Long, target: String, targetCount: Long)


  def auditDF(df: DataFrame)
  def processLogs(rdd: RDD[String])
  def updateMetainfo
  def putMetainfoObject(objectId: String, values: scala.collection.immutable.Map[String, String]): Boolean

  /**
    * Recreación del índice Solr con los conceptos de diccionario
    *
    * @param name - nombre del índice Solr
    */
  def recreateSolrDictionaryIndex(name: String): Unit
}
