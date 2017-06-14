package com.bluetab.matrioska.ingest.services

import org.apache.spark.sql.DataFrame

/**
 * Interfaz o Trait con los servicios del proyecto TickHistory2 que permite calcular el factor corrector histórico del valor de las acciones del IBEX35
 * @author xe58268
 *
 */
trait TickHistoryService {

  /**
   * @param events
   * @return
   */
  def transformEvents(events: DataFrame): DataFrame

  /**
   * @param dividends
   * @return
   */
  def transformDividends(dividends: DataFrame): DataFrame

  /**
   * @param dates
   * @param endOfDays
   * @param events
   * @param dividends
   * @return
   */
  def joinEventsAndDividendsByDateAndRic(dates: DataFrame, endOfDays: DataFrame, events: DataFrame, dividends: DataFrame): DataFrame

  /**
   * @param joinedData
   * @return
   */
  def calculateAdjustmentFactorData(joinedData: DataFrame): String
  /**
   * @param x
   */
  def saveAdjustmentFactorData(x: String)
}
