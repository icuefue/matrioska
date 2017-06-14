package com.bbva.ebdm.linx.ingest.services

import org.apache.spark.sql.DataFrame
import java.util.Date

/**
 * Interfaz o Trait con las servicios comunes del proyecto GSDS
 * @author xe58268
 *
 */
/**
 * @author xe58268
 *
 */
trait GsdsService {

  /**
   * Recupera la tabla completa(Ultima partición) de Eventos o splits (t_events) de las acciones
   * @return devuelve un DataFrame con la tabla de Eventos cargada
   */
  def loadEvents: DataFrame

  /**
   * Recupera la tabla completa(Última partición) con el valor de las acciones del IBEX 35 (t_endofdays)
   * @return devuelve un DataFrame con la tabla con el valor de las acciones(t_endofdays
   */
  def loadEndOfDays: DataFrame

  /**
   * Recupera la tabla completa(Última partición) de dividendos(t_dividends)
   * @return devuelve un DataFrame con la tabla con el valor de las dividendos(t_dividends)
   */
  def loadDividends: DataFrame

  /**
   * Recupera la tabla completa(Última partición) de rfq_ion(rfq_ion)
   * @return devuelve un DataFrame con la tabla con el valor de las rfq_ion(rfq_ion)
   */
  def loadRfqIon: DataFrame

  /**
   * Crea la vista de OrdersEurexIon en master teniendo en cuenta la fecha de última carga en raw
   *
   */
  def createMasterViewOrdersEurexIon

  /**
   * Crea la vista de RfqIon en Master teniendo en cuenta la fecha de última carga en raw
   *
   */
  def createMasterViewRfqIon

  /**
   * Crea la vista de RfqRet en Master teniendo en cuenta la fecha de última carga en raw
   *
   */
  def createMasterViewRfqRet

  /**
   * Crea la vista de TradesEurexIon en Master teniendo en cuenta la fecha de última carga en raw
   *
   */
  def createMasterViewTradesEurexIon

  /**
   * Borramos el flag pasado por parámetro
   *
   * @param path del flag pasado por parámetro
   */
  def deleteGsdsFlag(path: String)

  /**
   * Leemos y devolvemos el contenido del flag especificado
   *
   * @param path path del fichero flag que queremos leer
   * @return devuelve el contenido del fichero en formato Option[String]
   */
  def getGsdsFlagPipeline(path: String): Option[String]

  /**
   * Borra una partición de la tabla PriBonus de Gsds
   *
   * @param oDate fecha de la partición que hay que borrar
   */
  def deletePriBonusPartition(oDate: Date)

  /**
   * Carga la tabla PriBonus del Raw al Master
   * @param oDate fecha de la partición que hay que cargar
   */
  def loadTablePriBonus(oDate: Date)

  /**
   * Audita la inserción en la tabla PriBonus
   * @param oDate fecha de la partición que hay que cargar
   */
  def auditTablePriBonus(oDate: Date)

}