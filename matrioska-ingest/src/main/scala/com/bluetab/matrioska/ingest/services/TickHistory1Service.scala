package com.bluetab.matrioska.ingest.services

import org.apache.spark.sql.DataFrame

import java.util.Date

/**
 * Interfaz o Trait con los servicios del proyecto TickHistory que carga en master los datos obtenidos de Typhoon
 * @author xe54491
 *
 */
trait TickHistory1Service {

  /**
   * @param srcSchema
   * @param srcTable
   * @param planDate
   * @return
   *
   * TODO: Revisar si es necesario poner esta funcion en comun con la de GMCalculatedDataService
   */
  def loadHiveRawTablePartitionInDate(srcSchema: String, srcTable: String, planDate: Date): DataFrame

  /**
   * @param srcSchema
   * @param srcTable
   * @param months
   * @return
   *
   */
  def loadHiveMasterTablePartitionInMonths(srcSchema: String, srcTable: String, months: List[(String, String)]): DataFrame

  /**
   * @param DFToWrite
   * @param srcSchema
   * @param srcTable
   * @return
   *
   */
  def replaceMasterPartition(dfToWrite: DataFrame, tgtSchema: String, tgtTable: String)

}
