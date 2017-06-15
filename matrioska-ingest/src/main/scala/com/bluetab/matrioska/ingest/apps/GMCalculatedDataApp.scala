package com.bluetab.matrioska.ingest.apps

import java.text.{DateFormat, SimpleDateFormat}
import java.util.Date

import com.bluetab.matrioska.core.conf.{CoreContext, CoreServices}
import com.bluetab.matrioska.core.{LinxApp, LinxAppArgs}
import com.bluetab.matrioska.ingest.conf.IngestServices
import com.bluetab.matrioska.ingest.constants.IngestConstants
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row

object GMCalculatedDataAppConstants {
  val srcSchema = IngestConstants.RawSchemaRisks
  val tgtSchema = IngestConstants.MasterSchemaRisks
  val RiskApiTables: Seq[String] = Seq("t_gm_risks_cr01",
    "t_gm_risks_deltaeq",
    //"t_gm_risks_deltafx",
    "t_gm_risks_deltainfl",
    "t_gm_risks_dv01nb",
    "t_gm_risks_expfx",
    "t_gm_risks_pl",
    //"t_gm_risks_rmargin",
    "t_gm_risks_sdiv",
    "t_gm_risks_vegacrd",
    "t_gm_risks_vegaeq",
    "t_gm_risks_vegafx",
    "t_gm_risks_vegainfl",
    "t_gm_risks_vegair")
}

class GMCalculatedDataApp extends LinxApp {

  override def run(args: Seq[String]) {

    val formatter: DateFormat = new SimpleDateFormat("yyyyMMdd");
    val planDate = formatter.parse(LinxAppArgs.planDate);

//    loadTableTGMRisksPl(planDate)
//    loadTableTGMRisksDeltaEq(planDate)
//    loadTableTGMRisksVegaEq(planDate)
//    loadTableTGMRisksCr01(planDate)
//    loadTableTGMRisksVegaFx(planDate)
//    loadTableTGMRisksVegaIr(planDate)
//    loadTableTGMRisksExpFx(planDate)
//    loadTableTGMRisksDeltaInfl(planDate)
//    loadTableTGMRisksVegaInfl(CoreContext.sc.emptyRDD, planDate)
//    loadTableTGMRisksDeltaFx(planDate)
//    loadTableTGMRisksSdiv(planDate)
//    loadTableTGMRisksRmargin(planDate)
//    loadTableTGMRisksDv01Nb(planDate)
//    loadTableTGMRisksLogApi(planDate)
    applyApiLogErrors(planDate)

  }

  def genericTranformationsMaster(srcSchema: String, srcTable: String, tgtSchema: String, tgtTable: String, planDate: Date): Unit = {
    val rawTableDF = IngestServices.gmCalculatedDataService.loadHiveTablePartition(srcSchema, srcTable, planDate)
    if (!CoreServices.commonService.isEmptyDF(rawTableDF)) {
      val rddWithoutPartition = IngestServices.gmCalculatedDataService.dropAuditRawFields(rawTableDF.rdd)
      val rddWithoutPartitionAndSources = IngestServices.gmCalculatedDataService.dropSourceRawFields(rddWithoutPartition)
      val rddMaster = IngestServices.gmCalculatedDataService.commonInsertTransformations(rddWithoutPartitionAndSources)
      val dfTable = IngestServices.gmCalculatedDataService.createTableDataFrame(rddMaster, tgtSchema, tgtTable)
      val savedDF = IngestServices.gmCalculatedDataService.replaceMasterPartition(dfTable, tgtSchema, tgtTable)
    } else {
      CoreContext.logger.info("No existen datos en la tabla " + srcTable)

    }
  }

  def loadTableTGMRisksPl(planDate: Date): Unit = {
    val srcTable = "tgriskr_pl"
    val tgtTable = "t_gm_risks_pl"

    CoreContext.logger.info("Inicio de carga de la tabla " + srcTable + " en la tabla " + tgtTable)
    val dfTable = genericTranformationsMaster(GMCalculatedDataAppConstants.srcSchema, srcTable, GMCalculatedDataAppConstants.tgtSchema, tgtTable, planDate)
  }

  def loadTableTGMRisksDeltaEq(planDate: Date): Unit = {
    val srcTable = "tgriskr_deltaeq"
    val tgtTable = "t_gm_risks_deltaeq"

    CoreContext.logger.info("Inicio de carga de la tabla " + srcTable + " en la tabla " + tgtTable)
    val dfTable = genericTranformationsMaster(GMCalculatedDataAppConstants.srcSchema, srcTable, GMCalculatedDataAppConstants.tgtSchema, tgtTable, planDate)

  }

  def loadTableTGMRisksVegaEq(planDate: Date): Unit = {
    val srcTable = "tgriskr_vegaeq"
    val tgtTable = "t_gm_risks_vegaeq"

    CoreContext.logger.info("Inicio de carga de la tabla " + srcTable + " en la tabla " + tgtTable)
    val dfTable = genericTranformationsMaster(GMCalculatedDataAppConstants.srcSchema, srcTable, GMCalculatedDataAppConstants.tgtSchema, tgtTable, planDate)
  }

  def loadTableTGMRisksCr01(planDate: Date): Unit = {
    val srcTable = "tgriskr_cr01"
    val tgtTable = "t_gm_risks_cr01"

    CoreContext.logger.info("Inicio de carga de la tabla " + srcTable + " en la tabla " + tgtTable)
    val dfTable = genericTranformationsMaster(GMCalculatedDataAppConstants.srcSchema, srcTable, GMCalculatedDataAppConstants.tgtSchema, tgtTable, planDate)
  }

  def loadTableTGMRisksVegaFx(planDate: Date): Unit = {
    val srcTable = "tgriskr_vegafx"
    val tgtTable = "t_gm_risks_vegafx"

    CoreContext.logger.info("Inicio de carga de la tabla " + srcTable + " en la tabla " + tgtTable)
    val dfTable = genericTranformationsMaster(GMCalculatedDataAppConstants.srcSchema, srcTable, GMCalculatedDataAppConstants.tgtSchema, tgtTable, planDate)
  }

  def loadTableTGMRisksVegaIr(planDate: Date): Unit = {
    val srcTable = "tgriskr_vegair"
    val tgtTable = "t_gm_risks_vegair"

    CoreContext.logger.info("Inicio de carga de la tabla " + srcTable + " en la tabla " + tgtTable)
    val rawTableDF = IngestServices.gmCalculatedDataService.loadHiveTablePartition(GMCalculatedDataAppConstants.srcSchema, srcTable, planDate)
    if (!CoreServices.commonService.isEmptyDF(rawTableDF)) {
      val rddWithoutPartition = IngestServices.gmCalculatedDataService.dropAuditRawFields(rawTableDF.rdd)
      val rddWithoutPartitionAndSources = IngestServices.gmCalculatedDataService.dropSourceRawFields(rddWithoutPartition)

      val rddVegaInfl = IngestServices.gmCalculatedDataService.vegaDivisionInflacion(rddWithoutPartitionAndSources)
      if (!rddVegaInfl.isEmpty()) { //Llamamos para insertar las lineas que corresponden a VegaInfl
        loadTableTGMRisksVegaInfl(rddVegaInfl, planDate)
      }

      val rddVegaCredito = IngestServices.gmCalculatedDataService.vegaDivisionCredito(rddWithoutPartitionAndSources)
      if (!rddVegaCredito.isEmpty()) { //Llamamos para insertar las lineas que corresponden a VegaCredito
        loadTableTGMRisksVegaCrd(rddVegaCredito)
      }

      // Continuo la ejecucion de VegaIR
      val rddWithoutDiferentsVegas = IngestServices.gmCalculatedDataService.filterVegaDivisions(rddWithoutPartitionAndSources)
      val rddMaster = IngestServices.gmCalculatedDataService.commonInsertTransformations(rddWithoutDiferentsVegas)
      val dfTable = IngestServices.gmCalculatedDataService.createTableDataFrame(rddMaster, GMCalculatedDataAppConstants.tgtSchema, tgtTable)
      val savedDF = IngestServices.gmCalculatedDataService.replaceMasterPartition(dfTable, GMCalculatedDataAppConstants.tgtSchema, tgtTable)
    } else {
      CoreContext.logger.info("No existen datos en la tabla " + srcTable)
    }
  }

  def loadTableTGMRisksExpFx(planDate: Date): Unit = {
    val srcTable = "tgriskr_expfx"
    val tgtTable = "t_gm_risks_expfx"

    CoreContext.logger.info("Inicio de carga de la tabla " + srcTable + " en la tabla " + tgtTable)
    val rawTableDF = IngestServices.gmCalculatedDataService.loadHiveTablePartition(GMCalculatedDataAppConstants.srcSchema, srcTable, planDate)
    if (!CoreServices.commonService.isEmptyDF(rawTableDF)) {
      val rddWithoutPartition = IngestServices.gmCalculatedDataService.dropAuditRawFields(rawTableDF.rdd)
      val rddWithoutPartitionAndSources = IngestServices.gmCalculatedDataService.dropSourceRawFields(rddWithoutPartition)
      val rddVertical = IngestServices.gmCalculatedDataService.expFxTransformations(rddWithoutPartitionAndSources)
      val rddMaster = IngestServices.gmCalculatedDataService.commonInsertTransformations(rddVertical)
      val dfTable = IngestServices.gmCalculatedDataService.createTableDataFrame(rddMaster, GMCalculatedDataAppConstants.tgtSchema, tgtTable)
      val savedDF = IngestServices.gmCalculatedDataService.replaceMasterPartition(dfTable, GMCalculatedDataAppConstants.tgtSchema, tgtTable)
    } else {
      CoreContext.logger.info("No existen datos en la tabla " + srcTable)
    }

  }

  def loadTableTGMRisksDeltaInfl(planDate: Date): Unit = {
    val srcTable = "tgriskr_deltainfl"
    val tgtTable = "t_gm_risks_deltainfl"

    CoreContext.logger.info("Inicio de carga de la tabla " + srcTable + " en la tabla " + tgtTable)
    val dfTable = genericTranformationsMaster(GMCalculatedDataAppConstants.srcSchema, srcTable, GMCalculatedDataAppConstants.tgtSchema, tgtTable, planDate)
  }

  def loadTableTGMRisksVegaInfl(vegairRdd: RDD[Row], planDate: Date): Unit = {
    val srcTable = "tgriskr_vegainfl"
    val tgtTable = "t_gm_risks_vegainfl"

    CoreContext.logger.info("Inicio de carga de la tabla " + srcTable + " en la tabla " + tgtTable)
    if (vegairRdd.isEmpty()) {
      val dfTable = genericTranformationsMaster(GMCalculatedDataAppConstants.srcSchema, srcTable, GMCalculatedDataAppConstants.tgtSchema, tgtTable, planDate)
    } else { // Llamado desde VegaIr
      val changesvegaInfl = IngestServices.gmCalculatedDataService.selectGroupVegaInfl(vegairRdd)
      val rddMaster = IngestServices.gmCalculatedDataService.commonInsertTransformations(changesvegaInfl)
      val dfTable = IngestServices.gmCalculatedDataService.createTableDataFrame(rddMaster, GMCalculatedDataAppConstants.tgtSchema, tgtTable)
      val savedDF = IngestServices.gmCalculatedDataService.replaceMasterPartition(dfTable, GMCalculatedDataAppConstants.tgtSchema, tgtTable)
    }
  }

  def loadTableTGMRisksDeltaFx(planDate: Date): Unit = {
    val srcTable = "tgriskr_deltafx"
    val tgtTable = "t_gm_risks_deltafx"

    CoreContext.logger.info("Inicio de carga de la tabla " + srcTable + " en la tabla " + tgtTable)
    val rawTableDF = IngestServices.gmCalculatedDataService.loadHiveTablePartition(GMCalculatedDataAppConstants.srcSchema, srcTable, planDate)
    if (!CoreServices.commonService.isEmptyDF(rawTableDF)) {
      val rddfilterRows = IngestServices.gmCalculatedDataService.filterRowsDeltafx(rawTableDF.rdd)
      val rddTransformationsDeltafx = IngestServices.gmCalculatedDataService.transformationsDeltafx(rddfilterRows)
      val rddHorizonDate = IngestServices.gmCalculatedDataService.insertHorizonDate(rddTransformationsDeltafx)
      val rddFinalData = IngestServices.gmCalculatedDataService.dropSourceRawFields(rddHorizonDate)
      val rddMaster = IngestServices.gmCalculatedDataService.commonInsertTransformations(rddFinalData)
      val dfTable = IngestServices.gmCalculatedDataService.createTableDataFrame(rddMaster, GMCalculatedDataAppConstants.tgtSchema, tgtTable)
      val savedDF = IngestServices.gmCalculatedDataService.replaceMasterPartition(dfTable, GMCalculatedDataAppConstants.tgtSchema, tgtTable)
    } else {
      CoreContext.logger.info("No existen datos en la tabla " + srcTable)
    }
  }

  def loadTableTGMRisksSdiv(planDate: Date): Unit = {
    val srcTable = "tgriskr_sdiv"
    val tgtTable = "t_gm_risks_sdiv"

    CoreContext.logger.info("Inicio de carga de la tabla " + srcTable + " en la tabla " + tgtTable)
    val dfTable = genericTranformationsMaster(GMCalculatedDataAppConstants.srcSchema, srcTable, GMCalculatedDataAppConstants.tgtSchema, tgtTable, planDate)
  }

  def loadTableTGMRisksRmargin(planDate: Date): Unit = {
    val srcTable = "tgriskr_rmargin"
    val tgtTable = "t_gm_risks_rmargin"

    CoreContext.logger.info("Inicio de carga de la tabla " + srcTable + " en la tabla " + tgtTable)
    val dfTable = genericTranformationsMaster(GMCalculatedDataAppConstants.srcSchema, srcTable, GMCalculatedDataAppConstants.tgtSchema, tgtTable, planDate)
  }

  def loadTableTGMRisksDv01Nb(planDate: Date): Unit = {
    val srcTable = "tgriskr_dv01nb"
    val tgtTable = "t_gm_risks_dv01nb"

    CoreContext.logger.info("Inicio de carga de la tabla " + srcTable + " en la tabla " + tgtTable)
    val rawTableDF = IngestServices.gmCalculatedDataService.loadHiveTablePartition(GMCalculatedDataAppConstants.srcSchema, srcTable, planDate)
    if (!CoreServices.commonService.isEmptyDF(rawTableDF)) {
      val rddVertical = IngestServices.gmCalculatedDataService.dv01nbTransformations(rawTableDF.rdd)
      val dfTable = IngestServices.gmCalculatedDataService.createTableDataFrame(rddVertical, GMCalculatedDataAppConstants.tgtSchema, tgtTable)
      val savedDF = IngestServices.gmCalculatedDataService.replaceMasterPartition(dfTable, GMCalculatedDataAppConstants.tgtSchema, tgtTable)
    } else {
      CoreContext.logger.info("No existen datos en la tabla " + srcTable)
    }
  }

  // Solo es llamado desde VegaIr
  def loadTableTGMRisksVegaCrd(vegairRdd: RDD[Row]): Unit = {
    val tgtTable = "t_gm_risks_vegacrd"

    CoreContext.logger.info("Inicio de carga de la tabla " + tgtTable)
    val rddMaster = IngestServices.gmCalculatedDataService.commonInsertTransformations(vegairRdd)
    val dfTable = IngestServices.gmCalculatedDataService.createTableDataFrame(rddMaster, GMCalculatedDataAppConstants.tgtSchema, tgtTable)
    val savedDF = IngestServices.gmCalculatedDataService.replaceMasterPartition(dfTable, GMCalculatedDataAppConstants.tgtSchema, tgtTable)
  }

  def loadTableTGMRisksLogApi(planDate: Date): Unit = {
    val srcTable = "tgriskr_apilog"
    val tgtTable = "t_gm_risks_log_api"

    CoreContext.logger.info("Inicio de carga de la tabla " + srcTable + " en la tabla " + tgtTable)
    val rawTableDF = IngestServices.gmCalculatedDataService.loadHiveTablePartition(GMCalculatedDataAppConstants.srcSchema, srcTable, planDate)
    if (!CoreServices.commonService.isEmptyDF(rawTableDF)) {
      val rddWithoutPartition = IngestServices.gmCalculatedDataService.dropAuditRawFields(rawTableDF.rdd)
      val filteredRowsRdd = IngestServices.gmCalculatedDataService.apiLogErrorFilter(rddWithoutPartition)
      val apiLogReplaceDate = IngestServices.gmCalculatedDataService.apiLogReplaceDate(filteredRowsRdd, planDate)
      val dfTable = IngestServices.gmCalculatedDataService.createTableDataFrame(apiLogReplaceDate, GMCalculatedDataAppConstants.tgtSchema, tgtTable)
      val savedDF = IngestServices.gmCalculatedDataService.apiLogReplaceMasterPartition(dfTable, GMCalculatedDataAppConstants.tgtSchema, tgtTable)
    } else {
      CoreContext.logger.info("No existen datos en la tabla " + srcTable)
    }
  }

  def applyApiLogErrors(planDate: Date): Unit = {
    val srcTable = "t_gm_risks_log_api"

    val apiLogDF = IngestServices.gmCalculatedDataService.loadHiveMasterTablePartition(GMCalculatedDataAppConstants.tgtSchema, srcTable, planDate)
    val badNbDF = IngestServices.gmCalculatedDataService.distinctNBApiLog(apiLogDF)
    if (!CoreServices.commonService.isEmptyDF(badNbDF)) {
      val persistedBadNbDF = badNbDF.persist()
      GMCalculatedDataAppConstants.RiskApiTables.foreach { table =>
        CoreContext.logger.info("Inicio de aplicacion de Api Log para " + table)
        val DFToWrite = IngestServices.gmCalculatedDataService.applyApilog(persistedBadNbDF, table, planDate)
        IngestServices.gmCalculatedDataService.replaceMasterPartition(DFToWrite, GMCalculatedDataAppConstants.tgtSchema, table)
      }
    }
  }

}
