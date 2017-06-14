package com.bluetab.matrioska.core.beans

import scala.collection.mutable.{ListBuffer, Map}

case class DataFrameStats(name: String, count: Long, colStats: Map[String, ColumnStats])

case class ColumnStats(name: String, columnType: String, max: String,
  min: String, mean: String, stddev: String, distinctValues: String,
  countNotNulls: String, percentageNulls: String, freqItems: ListBuffer[FreqItem])

case class FreqItem(var item: String, var number: Long, var percentage: Double)
