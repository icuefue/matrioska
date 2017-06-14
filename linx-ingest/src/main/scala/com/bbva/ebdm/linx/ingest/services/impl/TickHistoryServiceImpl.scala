package com.bbva.ebdm.linx.ingest.services.impl

import java.text.SimpleDateFormat
import java.util.Calendar

import org.apache.spark.rdd.RDD.rddToPairRDDFunctions
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._

import com.bbva.ebdm.linx.core.conf.CoreContext
import com.bbva.ebdm.linx.core.conf.CoreRepositories
import com.bbva.ebdm.linx.ingest.constants.IngestDfsConstants
import com.bbva.ebdm.linx.ingest.services.TickHistoryService
import com.bbva.ebdm.linx.ingest.structs.GsdsStructs

class TickHistoryServiceImpl extends TickHistoryService {

  def transformEvents(events: DataFrame): DataFrame = {
    CoreContext.logger.info("Transformando Eventos")
    val simpleTransforms = events.filter(events("effectiveDate") !== (""))
      .dropDuplicates()
    val result = simpleTransforms.rdd.map(r => (r.getString(0) + ":" + r.getString(1) + ":" + r.getString(10), r))
      .groupByKey().map(t => {
        var selectedRow: Option[Row] = None
        t._2.foreach { x =>
          if (selectedRow == None) {
            selectedRow = Some(x)
          } else if (x.getString(2) == "F")
            selectedRow = Some(x)
        }
        selectedRow.get
      })
    CoreContext.sqlContext.createDataFrame(result, GsdsStructs.event)
  }

  def transformDividends(dividends: DataFrame): DataFrame = {
    CoreContext.logger.info("Transformando Dividendos")
    val simpleTransforms = dividends.filter(dividends("dividendCurrency") === "EUR")
      .filter(dividends("dividendPayDate") !== (""))
      .dropDuplicates()

    val result = simpleTransforms.rdd.map(r => (r.getString(0) + ":" + r.getInt(10), r))
      .groupByKey().map(t => {
        var selectedRow: Option[Row] = None
        t._2.foreach { x =>
          if (selectedRow == None) {
            selectedRow = Some(x)
          } else if (x.getString(2) == "F")
            selectedRow = Some(x)
        }
        selectedRow.get
      })
    CoreContext.sqlContext.createDataFrame(result, GsdsStructs.dividend)
  }

  def joinEventsAndDividendsByDateAndRic(dates: DataFrame, endOfDays: DataFrame, events: DataFrame, dividends: DataFrame): DataFrame = {
    CoreContext.logger.info("Realizando join fechas, endofdays, eventos, dividendos")
    val resultado = dates.as("a").join(endOfDays.as("b"), from_unixtime(unix_timestamp(col("b.tick_date"), "d-MMMM-yy"), "yyyyMMdd") === col("a.cod_fechanum"), "left_outer")
      .join(dividends.as("d"), from_unixtime(unix_timestamp(col("d.dividendPayDate"), "d-MMMM-yy"), "yyyyMMdd") === col("a.cod_fechanum") and col("d.ric") === col("b.ric"), "left_outer")
      .join(events.as("e"), from_unixtime(unix_timestamp(col("e.effectiveDate"), "d-MMMM-yy"), "yyyyMMdd") === col("a.cod_fechanum") and col("e.ric") === col("b.ric"), "left_outer")
      .filter(col("b.ric").isNotNull)
      .select("b.ric", "a.cod_fechanum", "b.closeBid", "d.dividendAmountRate", "e.adjustmentFactor")
    resultado
  }

  def calculateAdjustmentFactorData(joinedData: DataFrame): String = {

    CoreContext.logger.info("Calculando el factor de ajuste")
    val grouped = joinedData.rdd.map(r => (r.getString(0), r)).groupByKey()

    val resultado = new StringBuilder;
    grouped.collect.foreach(a => {
      val accion = a._1

      var factor = 1.0;
      val ordered = a._2.toList.sortBy(-_.getDouble(1).toInt)
      val calendar = Calendar.getInstance
      val format1 = new java.text.SimpleDateFormat("yyyyMMdd")
      var lastDay = format1.parse(ordered.head.getDouble(1).toInt.toString())
      val sdf = new SimpleDateFormat("yyyy-MM-dd");
      val firstDay = sdf.parse("2014-01-01");
      do {
        ordered.foreach(t => {
          val fechaFormateada = format1.parse(t.getDouble(1).toInt.toString())
          if (sdf.format(fechaFormateada).equals(sdf.format(lastDay))) {
            val dividendo = if (t.getString(3) == null || t.getString(3).isEmpty()) 0.0 else t.getString(3).toDouble
            val split = if (t.getString(4) == null || t.getString(4).isEmpty()) 0.0 else t.getString(4).toDouble
            if (dividendo != 0) {
              val precio = t.getDouble(2)
              factor = (1.0 - (dividendo.doubleValue() / precio.doubleValue())) * factor
            }
            if (split != 0)
              factor = factor * split.doubleValue()

          }
        })
        val linea = accion + "\t" + sdf.format(lastDay) + "\t" + factor + "\n"
        resultado.append(linea)
        calendar.setTime(lastDay);
        calendar.add(Calendar.DATE, -1);
        lastDay = calendar.getTime();
      } while (sdf.format(lastDay).compareTo(sdf.format(firstDay)) >= 0);

    })
    resultado.toString()
  }

  def saveAdjustmentFactorData(x: String) {

    val path = IngestDfsConstants.GsdsAdjustmentFactor
    CoreRepositories.dfsRepository.delete(path)
    CoreRepositories.dfsRepository.create(path, x.getBytes)

  }

}
