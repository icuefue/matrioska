package com.bluetab.matrioska.ingest.services.impl

import java.text.SimpleDateFormat
import java.text.DateFormat
import java.util.Date

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import com.bluetab.matrioska.core.conf.CoreRepositories
import com.bluetab.matrioska.core.conf.CoreRepositories
import com.bluetab.matrioska.core.conf.CoreServices
import com.bluetab.matrioska.core.enums.CompressionCodecs.CompressionCodec
import com.bluetab.matrioska.core.enums.CompressionCodecs.Snappy
import com.bluetab.matrioska.core.conf.{CoreConfig, CoreRepositories, CoreServices}
import com.bluetab.matrioska.core.enums.CompressionCodecs
import com.bluetab.matrioska.core.exceptions.FatalException
import com.bluetab.matrioska.ingest.constants.IngestDfsConstants
import com.bluetab.matrioska.ingest.services.TickHistory1Service
import com.bluetab.matrioska.ingest.structs.GsdsStructs

class TickHistory1ServiceImpl extends TickHistory1Service {

  private def dateToPartitionFields(date: Date): Array[String] = {
    val formatter: DateFormat = new SimpleDateFormat("yyyy-MM-dd")
    formatter.format(date).split("-")
  }

  def loadHiveRawTablePartitionInDate(srcSchema: String, srcTable: String, planDate: Date): DataFrame = {

    val where = {
      val dateArray = dateToPartitionFields(planDate)
      val day = "dia=" + dateArray(2)
      val month = "mes=" + dateArray(1)
      val year = "anio=" + dateArray(0)
    // This line removes the header of each file from the data
    // It seems that Spark is unable to do the removal of the header
    // even if it is configured in Hive to be done.
    // see https://issues.apache.org/jira/browse/SPARK-11374
      "(!(tick_date LIKE 'Date%')) and " + year + " and " + month + " and " + day
    }

    val partitionDF_raw : Option[DataFrame] = CoreRepositories.hiveRepository
      .sql("/hql/gmcalculateddata/load_table_data.hql", srcSchema, srcTable, where)

    partitionDF_raw match {
      case Some(partitionDF_raw) => {
        val partitionDF_noDate = partitionDF_raw.drop("dia").drop("anio").drop("mes")
        val partition_Indexed = if (srcTable.equals("t_timeandsales")) {
          partitionDF_noDate.withColumn("file_offset", monotonicallyIncreasingId)
        } else {
          partitionDF_noDate
        }
        val partitionDF_month = partition_Indexed.withColumn("year",
            from_unixtime(unix_timestamp(col("tick_date"), "dd-MMM-yyyy"), "yyyy"))
        val partitionDF = partitionDF_month.withColumn("month",
            from_unixtime(unix_timestamp(col("tick_date"), "dd-MMM-yyyy"), "MM"))
        partitionDF
      }
      case None => throw new FatalException("No se recuperan valores de " + srcSchema + "." + srcTable)
    }
  }

  def loadHiveMasterTablePartitionInMonths(srcSchema: String, srcTable: String, months: List[(String, String)]): DataFrame = {

    // This line removes the header of each file from the data
    // It seems that Spark is unable to do the removal of the header
    // even if it is configured in Hive to be done.
    // see https://issues.apache.org/jira/browse/SPARK-11374
    val startWhere = "(!(tick_date LIKE 'Date%')) and ("
    val where = months.foldLeft(startWhere)((a: String, b : (String, String)) => {
      val month = "month=" + b._2
      val year = "year=" + b._1
      val where = "(" + year + " and " + month + ")"
      if (a.equals(startWhere))
        a + where
      else
        a + " or " + where
    }) + ")"

    val partitionDF = CoreRepositories.hiveRepository
      .sql("/hql/gmcalculateddata/load_table_data.hql", srcSchema, srcTable, where)

    partitionDF match {
      case Some(partitionDF) => partitionDF
      case None => throw new FatalException("No se recuperan valores de " + srcSchema + "." + srcTable)
    }
  }

  def saveMasterTable(dfToWrite: DataFrame, tgtSchema: String, tgtTable: String): DataFrame = {
    val fullTableName = CoreConfig.hive.schemas.get(tgtSchema) + "." + tgtTable
    CoreRepositories.hiveRepository.setCompressionCodec(Some(Snappy))
    dfToWrite.repartition(8)
      .write
      .mode(SaveMode.Append)
      .partitionBy("year", "month")
      .saveAsTable(fullTableName)
    dfToWrite
  }


  def getDistinctMonths(inputDF : DataFrame) : List[(String, String)] = {
    // We take the year and month
    val monthsDF = inputDF.select("year","month")
    // We change that to a list containing year and month number in a tuple
    //val monthsList = monthsDF.rdd.map(r => (r(0).asInstanceOf[String], r(1).asInstanceOf[String])).collect().distinct.toList
    val monthsList = monthsDF.rdd.map(r => (r.getAs[String](0),r.getAs[String](1))).distinct.collect().toList
    monthsList
  }

  def replaceMasterPartition(dfToWrite: DataFrame, tgtSchema: String, tgtTable: String) = {

    if (!CoreServices.commonService.isEmptyDF(dfToWrite)) {

      CoreRepositories.hiveRepository.refreshTable(tgtSchema, tgtTable)

      val months : List[(String, String)] = getDistinctMonths(dfToWrite)

      val toUpdateDF = loadHiveMasterTablePartitionInMonths(
            tgtSchema,
            tgtTable,
            months)

      val oldFiles = toUpdateDF
        .select(input_file_name())
        .distinct()
        .collect()

      val totalResult = toUpdateDF.as("a").join(dfToWrite.select("ric", "tick_date").distinct.as("b"),
          (col("b.ric") <=> toUpdateDF("ric")) && (col("b.tick_date") <=> toUpdateDF("tick_date")), "left").
          where("b.ric is null and b.tick_date is null").
          select(col("a.*")).
          unionAll(dfToWrite)

      saveMasterTable(totalResult, tgtSchema, tgtTable)

      oldFiles.foreach { x =>
        val inputFileName = x.getAs[String](0)
        CoreRepositories.dfsRepository.deleteAbsolutePath(inputFileName)
      }

      CoreRepositories.impalaRepository.invalidate(tgtSchema, tgtTable)
    }
  }

}
