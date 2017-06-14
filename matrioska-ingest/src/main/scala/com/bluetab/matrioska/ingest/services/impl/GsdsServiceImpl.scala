package com.bluetab.matrioska.ingest.services.impl

import org.apache.spark.sql.DataFrame
import com.bluetab.matrioska.core.conf.CoreRepositories
import com.bluetab.matrioska.core.conf.CoreContext
import com.bluetab.matrioska.ingest.constants.IngestDfsConstants
import java.util.Date
import java.text.SimpleDateFormat
import java.util.Calendar

import com.bluetab.matrioska.core.LinxAppArgs
import com.bluetab.matrioska.core.conf.{CoreAppInfo, CoreRepositories}
import com.bluetab.matrioska.core.exceptions.FatalException
import com.bluetab.matrioska.core.{LinxApp, LinxAppArgs}
import com.bluetab.matrioska.ingest.constants.{IngestConstants, IngestDfsConstants}
import com.bluetab.matrioska.ingest.services.GsdsService
import com.bluetab.matrioska.ingest.structs.GsdsStructs

class GsdsServiceImpl extends GsdsService {

  def loadStocks: Option[DataFrame] = {
    CoreRepositories.hiveRepository.sql(IngestConstants.HqlGsdsStocks)
  }

  def loadEvents: DataFrame = {
    val maxDate = CoreRepositories.hiveRepository.sql(IngestConstants.HqlGsdsEventsMaxDate)
    var events: Option[DataFrame] = None
    maxDate match {
      case Some(maxDate) =>

        val row = maxDate.first
        val anio = row.getString(0)
        val mes = row.getString(1)
        val dia = row.getString(2)

        val path = IngestDfsConstants.GsdsEvents + "/anio=" + anio + "/mes=" + mes + "/dia=" + dia + "/"

        val eventsDF = CoreRepositories.dfsRepository.readCsvAsDF(path, GsdsStructs.event)
        events = Some(eventsDF)

      case None => throw new FatalException("No tenemos MaxDate para la tabla Events")
    }
    if (events == None)
      throw new FatalException("No se recuperan eventos")

    return events.get
  }

  def loadEndOfDays: DataFrame = {
    val result = CoreRepositories.hiveRepository.sql(IngestConstants.HqlGsdsEndOfDays)
    if (result == None)
      throw new FatalException("No se recuperan valores de endofday")
    CoreContext.sqlContext.createDataFrame(result.get.rdd, GsdsStructs.endofdays)
  }

  def loadDividends: DataFrame = {
    val maxDate = CoreRepositories.hiveRepository.sql(IngestConstants.HqlGsdsDividendsMaxDate)
    var dividends: Option[DataFrame] = None
    maxDate match {
      case Some(maxDate) =>

        val row = maxDate.first
        val anio = row.getString(0)
        val mes = row.getString(1)
        val dia = row.getString(2)

        val path = IngestDfsConstants.GsdsDividends + "/anio=" + anio + "/mes=" + mes + "/dia=" + dia + "/"

        val dividendsDF = CoreRepositories.dfsRepository.readCsvAsDF(path, GsdsStructs.dividend)
        dividends = Some(dividendsDF)

      case None => throw new FatalException("No tenemos MaxDate para la tabla Dividends")
    }
    if (dividends == None)
      throw new FatalException("No se recuperan Dividendos")

    return dividends.get
  }

  def loadRfqIon: DataFrame = {
    val maxDate = CoreRepositories.hiveRepository.sql(IngestConstants.HqlGsdsRfqionMaxDate)
    var data: Option[DataFrame] = None
    maxDate match {
      case Some(maxDate) =>

        val row = maxDate.first
        val anio = row.getString(0)
        val mes = row.getString(1)
        val dia = row.getString(2)

        val path = IngestDfsConstants.GsdsRfqion + "/anio=" + anio + "/mes=" + mes + "/dia=" + dia + "/"

        val eventsDF = CoreRepositories.dfsRepository.readCsvAsDF(path, GsdsStructs.rfqion, ";", true)
        data = Some(eventsDF)

      case None => throw new FatalException("No tenemos MaxDate para la tabla RfqIon")
    }
    if (data == None)
      throw new FatalException("No se recuperan eventos")

    return data.get
  }

  def createMasterViewOrdersEurexIon = {
    val result = CoreRepositories.hiveRepository.sql(IngestConstants.HqlGsds001MaxFecConc)
    result match {
      case Some(result) =>
        val maxDate = result.first.getString(0)
        val year = maxDate.substring(0, 4)
        val month = maxDate.substring(4, 6)
        val day = maxDate.substring(6, 8)
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlGsds001CreateView, year, month, day)
      case None =>
    }
  }

  def createMasterViewRfqIon = {
    val result = CoreRepositories.hiveRepository.sql(IngestConstants.HqlGsds002MaxFecConc)
    result match {
      case Some(result) =>
        val maxDate = result.first.getString(0)
        val year = maxDate.substring(0, 4)
        val month = maxDate.substring(4, 6)
        val day = maxDate.substring(6, 8)
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlGsds002CreateView, year, month, day)
      case None =>
    }
  }

  def createMasterViewRfqRet = {
    val result = CoreRepositories.hiveRepository.sql(IngestConstants.HqlGsds003MaxFecConc)
    result match {
      case Some(result) =>
        val maxDate = result.first.getString(0)
        val year = maxDate.substring(0, 4)
        val month = maxDate.substring(4, 6)
        val day = maxDate.substring(6, 8)
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlGsds003CreateView, year, month, day)
      case None =>
    }
  }

  def createMasterViewTradesEurexIon = {
    val result = CoreRepositories.hiveRepository.sql(IngestConstants.HqlGsds004MaxFecConc)
    result match {
      case Some(result) =>
        val maxDate = result.first.getString(0)
        val year = maxDate.substring(0, 4)
        val month = maxDate.substring(4, 6)
        val day = maxDate.substring(6, 8)
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlGsds004CreateView, year, month, day)
      case None =>
    }
  }

  def getGsdsFlagPipeline(path: String): Option[String] = {
    var result: Option[String] = None
    val flagFile = CoreRepositories.dfsRepository.textFile(path).collect()
    if (flagFile.length > 0) {
      result = Some(flagFile(0))
    }
    return result
  }

  def deleteGsdsFlag(path: String) = {
    CoreRepositories.dfsRepository.delete(path)
  }

  def deletePriBonusPartition(oDate: Date) = {
    val auxDate = new SimpleDateFormat("yyyyMMdd").format(oDate)
    CoreRepositories.dfsRepository.delete(IngestDfsConstants.GsdsTPriBonusPath + "/fechaproceso="
      + auxDate)
    CoreRepositories.hiveRepository.sql(IngestConstants.HqlGsds006DropPartitionPribonus, auxDate)
  }

  def loadTablePriBonus(oDate: Date) = {
    val auxDate = new SimpleDateFormat("yyyyMMdd").format(oDate)
    val cal = Calendar.getInstance();
    cal.setTime(oDate);
    val year = cal.get(Calendar.YEAR).toString;
    val month = cal.get(Calendar.MONTH).toString;
    val day = cal.get(Calendar.DAY_OF_MONTH).toString;
    CoreRepositories.hiveRepository.sql(IngestConstants.HqlGsds006CreateFunctionRowSequence)
    CoreRepositories.hiveRepository.sql(IngestConstants.HqlGsds006LoadTablePribonus, auxDate, year, month, day)
    CoreRepositories.impalaRepository.invalidate("md_gsds", "t_pri_bonus")
  }

  def auditTablePriBonus(oDate: Date) = {
    val fechaCarga = new SimpleDateFormat("yyyyMMdd").format(oDate)
    val fechaMillis = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss:SSS").format(new Date)
    CoreRepositories.hiveRepository.sql(IngestConstants.HqlGsds006AuditTablePribonus, fechaCarga, fechaMillis,
      CoreAppInfo.uuid, CoreAppInfo.malla, CoreAppInfo.job, CoreAppInfo.useCase, LinxAppArgs.user, LinxAppArgs.appName)
    CoreRepositories.impalaRepository.invalidate("rd_ebdmau", "t_audit_master")
  }

}
