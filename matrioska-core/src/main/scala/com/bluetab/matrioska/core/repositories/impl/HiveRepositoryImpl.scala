package com.bluetab.matrioska.core.repositories.impl

import com.bluetab.matrioska.core.conf.{CoreAppInfo, CoreConfig, CoreContext}
import com.bluetab.matrioska.core.enums.CompressionCodecs.{CompressionCodec, Gzip, Lzo, Snappy}
import com.bluetab.matrioska.core.exceptions.FatalException
import com.bluetab.matrioska.core.repositories.HiveRepository
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.SaveMode
import com.bluetab.matrioska.core.utils.GovernmentUtils

object HiveRepositoryConstants {

 val ContextCompressionCodec = "spark.sql.parquet.compression.codec"
 val HiveContextCompressionCodec = "parquet.compression";
}

class HiveRepositoryImpl extends HiveRepository {

  def getSchema(schema: String): Option[String] = {
    val result = CoreConfig.hive.schemas.get(schema)
    return Some(result)
  }

  def sql(path: String, args: String*): Option[DataFrame] = {

    val is = getClass().getResourceAsStream(path)
    if (is == null) {
      throw new FatalException("No existe el fichero: " + path)
    }
    var query = scala.io.Source.fromInputStream(is).mkString

    var i = 1
    for (arg <- args) {
      query = query.replace("$" + i, arg)
      i = i + 1
    }

    val keySet = CoreConfig.hive.schemas.keySet().toArray()
    for (keyAux <- keySet) {
      val key = keyAux.asInstanceOf[String]
      val pattern = "(?i)\\{" + key + "\\}"
      query = query.replaceAll(pattern, CoreConfig.hive.schemas.get(key))
    }

    if (!CoreAppInfo.notFound) {
      CoreContext.logger.info(s"Hive Query File: $path")
      CoreContext.logger.debug("Hive Query: " + query)
      GovernmentUtils.trazabilityLog("SRC", "SQL", query)
    }
    Some(CoreContext.hiveContext.sql(query));
  }

  def table(schema: String, table: String): DataFrame = {
    val finalSchema = CoreConfig.hive.schemas.get(schema)
    GovernmentUtils.trazabilityLog("SRC", "TABLE", s"$finalSchema.$table")
    CoreContext.hiveContext.table(s"$finalSchema.$table")
  }

  def createDataFrame(rdd: RDD[Row], struct: StructType): DataFrame = {
    CoreContext.hiveContext.createDataFrame(rdd, struct)
  }

  def setCompressionCodec(compressionCodec: Option[CompressionCodec]) = {

    compressionCodec match {
      case Some(Snappy) =>
        CoreContext.hiveContext.setConf(HiveRepositoryConstants.ContextCompressionCodec, "snappy")
        CoreContext.hiveContext.setConf(HiveRepositoryConstants.HiveContextCompressionCodec, "SNAPPY")
      case Some(Gzip) =>
        CoreContext.hiveContext.setConf(HiveRepositoryConstants.ContextCompressionCodec, "gzip")
        CoreContext.hiveContext.setConf(HiveRepositoryConstants.HiveContextCompressionCodec, "GZIP")
      case Some(Lzo) =>
        CoreContext.hiveContext.setConf(HiveRepositoryConstants.ContextCompressionCodec, "lzo")
        CoreContext.hiveContext.setConf(HiveRepositoryConstants.HiveContextCompressionCodec, "LZO")
      case None =>
        CoreContext.hiveContext.setConf(HiveRepositoryConstants.ContextCompressionCodec, "uncompressed")
        CoreContext.hiveContext.setConf(HiveRepositoryConstants.HiveContextCompressionCodec, "UNCOMPRESSED")
    }

  }
  
  def refreshTable(schemaName: String, tableName: String): Unit = {
    val finalSchema = CoreConfig.hive.schemas.get(schemaName)
    CoreContext.hiveContext.refreshTable(s"$finalSchema.$tableName")
  }

  def saveToTable(DFToWrite: DataFrame, tgtSchema: String, tgtTable: String, partition: Seq[String]) = {
    val fullTableName = CoreConfig.hive.schemas.get(tgtSchema) + "." + tgtTable
    DFToWrite.repartition(8)
      .write
      .mode(SaveMode.Append)
      .partitionBy(partition: _*)
      .saveAsTable(fullTableName)
    GovernmentUtils.trazabilityLog("TGT", "TABLE", fullTableName)
  }

  def saveToTable(DFToWrite: DataFrame, tgtSchema: String, tgtTable: String) = {
    val fullTableName = CoreConfig.hive.schemas.get(tgtSchema) + "." + tgtTable
    DFToWrite.repartition(8)
      .write
      .mode(SaveMode.Append)
      .saveAsTable(fullTableName)
    GovernmentUtils.trazabilityLog("TGT", "TABLE", fullTableName)
  }

}
