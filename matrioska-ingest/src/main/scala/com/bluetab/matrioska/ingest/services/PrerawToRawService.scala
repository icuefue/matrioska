package com.bluetab.matrioska.ingest.services

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.rdd.RDD
import com.bluetab.matrioska.ingest.beans.Source
import com.bluetab.matrioska.ingest.beans.Mask
import com.bluetab.matrioska.ingest.beans.AuditFileData
import com.bluetab.matrioska.ingest.beans.AuditFileData

trait PrerawToRawService {

  def getToLoadFilenames: Seq[String]

  def markFilesAsInProgress(filenames: Seq[String])

  def markFileAsInProgress(filename: String)

  def markAuditFilesAsFailure(files: Seq[AuditFileData])

  def markFilesAsFailure(filenames: Seq[String])

  def markFileAsFailure(filename: String)

  def markFilesAsSuccessful(filenames: Seq[String])

  def markFileAsSuccessful(filename: String)

  def loadFile(filename: String, mask: Mask, tableStruct: StructType): RDD[Tuple2[Seq[Any], Int]]

  def checkQuality(df: DataFrame, filename: String)

  def checkInProgress: Seq[String]

  def saveToTable(rdd: RDD[Tuple2[Seq[Any], Int]], mask: Mask, tableStruct: StructType, filenames: Seq[String]): (Seq[String], Seq[AuditFileData])

  def deleteDirectories(filenames: Seq[String])

  def sendErrorNofitication(errorMessage: String)

}
