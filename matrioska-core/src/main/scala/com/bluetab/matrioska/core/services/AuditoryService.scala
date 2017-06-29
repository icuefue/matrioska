package com.bluetab.matrioska.core.services

import com.bluetab.matrioska.core.beans.DataFrameStats
import org.apache.spark.sql.DataFrame

trait AuditoryService {

  def auditTables(tables: Array[String])

  def auditTable(schema: String, table: String)

  def auditDF(name: String, df: DataFrame): DataFrameStats

  def saveAsTableStats(tableStats: DataFrameStats)

  def saveAsFileStats(tableStats: DataFrameStats)

}
