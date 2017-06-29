package com.bluetab.matrioska.core.services.impl

import com.bluetab.matrioska.core.beans.{ColumnStats, DataFrameStats, FreqItem}
import com.bluetab.matrioska.core.conf.{CoreContext, CoreRepositories}
import com.bluetab.matrioska.core.services.AuditoryService
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

import scala.collection.mutable.ListBuffer
import scala.util.Try

object AuditoryConstants {
  val Schema = "EBDM"
  val TableStatsTable = "stats_table"
  val TableStatsTableColumn = "stats_table_column"
  val TableStatsFile = "stats_file"
  val TableStatsFileColumn = "stats_file_column"
}

class AuditoryServiceImpl extends AuditoryService {

  def auditTables(tables: Array[String]) = {

    for (table <- tables) {

      val tableData = table.split('.')
      if (tableData.length == 2)
        auditTable(tableData(0), tableData(1))
    }

  }

  def auditTable(schema: String, table: String) = {

    val df = CoreRepositories.hiveRepository.table(schema, table)
    val dfStats = auditDF(s"$schema.$table", df)
    saveAsTableStats(dfStats)
  }

  def auditDF(name: String, df: DataFrame): DataFrameStats = {

    val columnsMap = scala.collection.mutable.Map[String, ColumnStats]()

    val dataFrameStats = new DataFrameStats(name, df.count, columnsMap)

    val columns = df.dtypes

    for ((columnName, columnType) <- columns) {

      try {

        var columnMean, columnMin, columnMax, distinctValues, countNotNulls, percentageNulls = ""
        var freqItems: ListBuffer[FreqItem] = ListBuffer[FreqItem]()
        if (columnType == "DoubleType" || columnType == "IntegerType" || columnType == "LongType") {
          val result = Try(df
            .select(mean(df(columnName)).cast(StringType), min(df(columnName)).cast(StringType),
              max(df(columnName)).cast(StringType), countDistinct(columnName).cast(StringType),
              count(df(columnName)).cast(StringType), count("*").cast(StringType)).take(1)).toOption

          result match {
            case Some(result) =>
              if (result != null && result.length == 1) {
                columnMean = result(0).getString(0)
                columnMin = result(0).getString(1)
                columnMax = result(0).getString(2)
                distinctValues = result(0).getString(3)
                countNotNulls = result(0).getString(4)
                percentageNulls = ((dataFrameStats.count.toDouble - countNotNulls.toDouble) * 100).toString()
              }

            case None =>
          }
        } else {
          val result = df
            .select(count(df(columnName)).cast(StringType), countDistinct(columnName).cast(StringType)).take(1)

          if (result.length == 1) {
            countNotNulls = result(0).getString(0)
            distinctValues = result(0).getString(1)
            percentageNulls = ((dataFrameStats.count.toDouble - countNotNulls.toDouble) * 100).toString()

          }
        }

        val dfFreqItems = df.groupBy(columnName).count().as("count").sort(desc("count"))
          .select(df(columnName).cast(StringType), col("count")).take(20)
        for (freqItem <- dfFreqItems) {
          val item = freqItem.getString(0)
          val number = freqItem.getLong(1)
          val percentage = if (dataFrameStats.count != 0) (number.toDouble / dataFrameStats.count.toDouble) * 100.toDouble else 0.0
          val auditFreqItem = new FreqItem(item, number, percentage)
          freqItems.+=(auditFreqItem)
        }
        columnsMap.put(columnName, ColumnStats(columnName, columnType, columnMax, columnMin, columnMean, "", distinctValues, countNotNulls, percentageNulls, freqItems))
      } catch {
        case t: Throwable =>
          CoreContext.logger.warn("Ha fallado la introducción de estadísticas para la columna: " + columnName)
      }
    }

    dataFrameStats
  }

  def saveStats(tableStats: DataFrameStats, table: String, tableColumns: String) {

    var contentTable = (tableStats.name, Map(("stats", Map(("count", tableStats.count.toString())))))

    CoreRepositories.hBaseRepository.write(AuditoryConstants.Schema, table, contentTable)

    for ((colName, colStats) <- tableStats.colStats) {
      val colsMap: scala.collection.mutable.Map[String, String] = scala.collection.mutable.Map()
      colsMap.put("name", colStats.name)
      colsMap.put("min", colStats.min)
      colsMap.put("max", colStats.max)
      colsMap.put("mean", colStats.mean)
      colsMap.put("stddev", colStats.stddev)
      colsMap.put("countNotNulls", colStats.countNotNulls)
      colsMap.put("distinctValues", colStats.distinctValues)
      colsMap.put("countNotNulls", colStats.percentageNulls)
      colsMap.put("freqItems", CoreRepositories.jsonRepository.serialize(colStats.freqItems))

      val aux = Map(("stats", colsMap.toMap))
      val contentdd = (tableStats.name + "." + colName, aux)
      CoreRepositories.hBaseRepository.write(AuditoryConstants.Schema, tableColumns, contentdd)
    }

  }

  def saveAsTableStats(tableStats: DataFrameStats) = {
    saveStats(tableStats, AuditoryConstants.TableStatsTable, AuditoryConstants.TableStatsTableColumn)
  }

  def saveAsFileStats(tableStats: DataFrameStats) = {
    saveStats(tableStats, AuditoryConstants.TableStatsFile, AuditoryConstants.TableStatsFileColumn)
  }

}
