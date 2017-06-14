package com.bbva.ebdm.linx.core.repositories.impl

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.StructType

import com.bbva.ebdm.linx.core.conf.CoreAppInfo
import com.bbva.ebdm.linx.core.conf.CoreConfig
import com.bbva.ebdm.linx.core.conf.CoreContext
import com.bbva.ebdm.linx.core.exceptions.FatalException
import com.bbva.ebdm.linx.core.repositories.HiveRepository

import com.bbva.ebdm.linx.core.enums.CompressionCodecs.CompressionCodec
import com.bbva.ebdm.linx.core.enums.CompressionCodecs.Snappy
import com.bbva.ebdm.linx.core.enums.CompressionCodecs.Lzo
import com.bbva.ebdm.linx.core.enums.CompressionCodecs.Gzip

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
    }
    Some(CoreContext.hiveContext.sql(query));
  }

  def table(schema: String, table: String): DataFrame = {
    val finalSchema = CoreConfig.hive.schemas.get(schema)
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

}
