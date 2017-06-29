package com.bluetab.matrioska.ingest.services

import org.apache.spark.sql.DataFrame
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import java.util.Date

/**
 * Interfaz o Trait con las servicios comunes del proyecto GM Calculated Data
 * @author xe30842
 *
 */
trait GMCalculatedDataService {

  def loadHiveTable(srcSchema: String, srcTable: String): DataFrame

  def loadHiveTablePartition(srcSchema: String, srcTable: String, planDate: Date): DataFrame
  
  def loadHiveMasterTablePartition(srcSchema: String, srcTable: String, planDate: Date): DataFrame

  def dropAuditRawFields(rddRaw: RDD[Row]): RDD[Row]

  def dropSourceRawFields(rddRaw: RDD[Row]): RDD[Row]

  def commonInsertTransformations(rddtable: RDD[Row]): RDD[Row]

  def insertHorizonDate(rddRawTable: RDD[Row]): RDD[Row]

  def dv01nbTransformations(rddRawTable: RDD[Row]): RDD[Row]

  def vegaDivisionInflacion(cleanRdd: RDD[Row]): RDD[Row]

  def vegaDivisionCredito(cleanRdd: RDD[Row]): RDD[Row]

  def filterVegaDivisions(cleanRdd: RDD[Row]): RDD[Row]

  def selectGroupVegaInfl(cleanRdd: RDD[Row]): RDD[Row]

  def expFxTransformations(cleanRdd: RDD[Row]): RDD[Row]

  def filterRowsDeltafx(rddRawTable: RDD[Row]): RDD[Row]

  def transformationsDeltafx(cleanRdd: RDD[Row]): RDD[Row]

  def saveMasterTable(DFToWrite: DataFrame, tgtSchema: String, tgtTable: String): DataFrame

  def apiLogErrorFilter(cleanRdd: RDD[Row]): RDD[Row]

  def apiLogReplaceDate(cleanRdd: RDD[Row], planDate:Date): RDD[Row]

  def distinctNBApiLog(apiLogDF:DataFrame):DataFrame

  def applyApilog(badNbDF:DataFrame, table:String, planDate:Date):DataFrame

  def apiLogReplaceMasterPartition(DFToWrite: DataFrame, tgtSchema: String, tgtTable: String):DataFrame

  def replaceMasterPartition(DFToWrite: DataFrame,  tgtSchema: String, tgtTable:String):DataFrame

  def createTableDataFrame(rdd: RDD[Row], tgtSchema: String, tgtTable: String): DataFrame

}
