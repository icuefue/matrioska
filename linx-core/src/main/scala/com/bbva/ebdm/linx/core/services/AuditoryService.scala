package com.bbva.ebdm.linx.core.services

import org.apache.spark.sql.DataFrame
import com.bbva.ebdm.linx.core.beans.DataFrameStats

trait AuditoryService {

  def auditTables(tables: Array[String])

  def auditTable(schema: String, table: String)

  def auditDF(name: String, df: DataFrame): DataFrameStats

  def saveAsTableStats(tableStats: DataFrameStats)

  def saveAsFileStats(tableStats: DataFrameStats)

}
