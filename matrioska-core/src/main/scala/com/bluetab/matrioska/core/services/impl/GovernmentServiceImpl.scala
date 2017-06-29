package com.bluetab.matrioska.core.services.impl

import org.apache.commons.lang.exception.ExceptionUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SaveMode}
import org.apache.spark.sql.types.StructType
import org.slf4j.MDC
//import com.bluetab.matrioska.core.beans.Destination
//import com.bluetab.matrioska.core.beans.Item
//import com.bluetab.matrioska.core.beans.RdbmsOptions
//import com.bluetab.matrioska.core.beans.UseCase
//import com.bluetab.matrioska.core.conf.CoreContext
//import com.bluetab.matrioska.core.conf.CoreRepositories
//import com.bluetab.matrioska.core.conf.CoreServices
//import com.bluetab.matrioska.core.exceptions.TableNotFoundException
//import com.bluetab.matrioska.core.services.GovernmentService
//import com.bluetab.matrioska.core.conf.CoreConfig
//import com.bluetab.matrioska.core.constants.CoreDfsConstants

import com.bluetab.matrioska.core.beans._
import com.bluetab.matrioska.core.conf._
import com.bluetab.matrioska.core.constants.{CoreConstants, CoreDfsConstants}
import com.bluetab.matrioska.core.enums.PartitionTypes.{Fechaproceso, NoPartition, PartitionType, YearMonthDay}
import com.bluetab.matrioska.core.exceptions.TableNotFoundException
import com.bluetab.matrioska.core.services.GovernmentService
import com.bluetab.matrioska.core.structs.GovernmentStructs
import com.bluetab.matrioska.core.utils.GovernmentUtils

import scala.collection.mutable

object GovernmentConstants {
  val AuditSchema = "rd_ebdmau"
  val AuditTable = "t_audit_raw"
  val LogExecutionsTable = "t_log_executions"
  val LogDetailsTable = "t_log_details"
  val FechaProceso = "fechaproceso"
  val FirstTimeDate = "1970-01-01 00:00:00"
}

object MetaInfoConstants {

  val Source = "oracle.ekev"
  val TableSchema = "EKEV"
  val TableNames = s"$TableSchema.TEKEVDIN" :: s"$TableSchema.TEKEVPTN" :: s"$TableSchema.TEKEVNAV" :: Nil
  val TargetDirs = CoreDfsConstants.TablesTEbdmgv01Tekevdin ::
    CoreDfsConstants.TablesTEbdmgv01Tekevptn :: CoreDfsConstants.TablesTEbdmgv05Tekevnav :: Nil
  val TargetDatabaseName = "rd_ebdmgv"
  val TargetTableNames = "t_ebdmgv01_tekevdin" :: "t_ebdmgv01_tekevptn" :: "t_ebdmgv05_tekevnav" :: Nil

}

class GovernmentServiceImpl extends GovernmentService {

  MDC.put("header", LogHeaderEnum.DETAIL.toString)

  override def getTableDefinition(schema: String, table: String): StructType = {
    val t = CoreRepositories.hiveRepository.sql(CoreConstants.HqlGovernmentGetTableStructure, schema, table)
    t match {
      case Some(t) => t.schema
      case None => throw new TableNotFoundException(s"La tabla $schema.$table no existe")
    }

  }

  override def getTableCount(schema: String, table: String, partitionType: PartitionType, planDate: String): Long = {
    var dataFrame: Option[DataFrame] = None
    partitionType match {
      case NoPartition => dataFrame = CoreRepositories.hiveRepository.sql(CoreConstants.HqlGovernmentGetTableCount, schema, table, "")
      case Fechaproceso => dataFrame = CoreRepositories.hiveRepository.sql(CoreConstants.HqlGovernmentGetTableCount, schema, table,
        s"where fechaproceso = '$planDate'")
      case YearMonthDay => dataFrame = CoreRepositories.hiveRepository.sql(CoreConstants.HqlGovernmentGetTableCount, schema, table,
        s"where year = '${planDate.substring(0,4).toInt}" + s"'" +
          s" and month = '${planDate.substring(4,6).toInt}'" +
          s" and day = '${planDate.substring(6,8).toInt}'")
    }

    dataFrame match {
      case Some(count) => count.take(1)(0).getLong(0)
      case None => throw new TableNotFoundException(s"La tabla $schema.$table no existe")
    }
  }

  override def findAppInfo(name: String): Option[AppInfo] = {

    val queryResult = CoreRepositories.hiveRepository.sql(CoreConstants.HqlGovernmentGetAppInfo, name).get.collect()
    var result: Option[AppInfo] = None

    if (queryResult.length == 1) {
      var appInfo = new AppInfo
      appInfo.name = queryResult(0).getString(2)
      appInfo.malla = queryResult(0).getString(0)
      appInfo.job = queryResult(0).getString(1)
      appInfo.capa = queryResult(0).getString(3)
      appInfo.useCase = queryResult(0).getString(4)
      appInfo.uuid = CoreContext.sc.applicationId

      result = Some(appInfo)
    }
    return result
  }

  override def findUseCases: mutable.Map[String, UseCase] = {

    val item1 = Item("uno.*", Destination("HIVE", "ud_xe58268_test.data_ingestion_test_table_uno"))
    val item2 = Item("dos.*", Destination("HIVE", "ud_xe58268_test.data_ingestion_test_table_dos"))
    val itemsHola = Seq(item1, item2)
    val useCaseHola = UseCase("caso_uso_1", itemsHola)
    val item3 = Item(".+", Destination("HIVE", "ud_xe58268_test.data_ingestion_test_table_tres_y_cuatro"))
    val itemsAdios = Seq(item3)
    val useCaseAdios = UseCase("caso_uso_2", itemsAdios)
    val useCases = mutable.Map[String, UseCase]()

    useCases.put("caso_uso_1", useCaseHola)
    useCases.put("caso_uso_2", useCaseAdios)
    useCases
  }

  override def logOnStart {
    MDC.put("estado", LogStatusEnum.RUNNING.toString)
    MDC.put("header", LogHeaderEnum.EXECUTION.toString)
    CoreContext.logger.info("App running")
    MDC.put("header", LogHeaderEnum.DETAIL.toString)
  }

  override def logOnComplete {
    MDC.put("estado", LogStatusEnum.OK.toString)
    MDC.put("header", LogHeaderEnum.EXECUTION.toString)
    CoreContext.logger.info("App OK")
    MDC.put("header", LogHeaderEnum.DETAIL.toString)
  }
  override def logOnError(exception: Throwable) {
    if (CoreAppInfo.notFound) logOnStart
    MDC.put("estado", LogStatusEnum.KO.toString)
    MDC.put("header", LogHeaderEnum.EXECUTION.toString)
    CoreContext.logger.error(ExceptionUtils.getStackTrace(exception))
    MDC.put("header", LogHeaderEnum.DETAIL.toString)
  }

  override def auditRawTable(options: RdbmsOptions) = {

    val sourceCount = CoreRepositories.dfsRepository.textFile(options.targetDir).count()
    val destCount = CoreServices.governmentService.getTableCount(options.targetSchema, options.targetTable,
      options.partitionType, options.planDate)

    auditRawTable(options.sourceSchema + "." + options.sourceTable, sourceCount,
      options.targetSchema + "." + options.targetTable, destCount)
  }

  override def auditRawTable(source: String, sourceCount: Long, target: String, targetCount: Long) = {
    val auditMsg = source + "|" + sourceCount + "|" + target + "|" + targetCount
    MDC.put("header", LogHeaderEnum.AUDITRAW.toString)
    CoreContext.logger.info(auditMsg)
    MDC.put("header", LogHeaderEnum.DETAIL.toString)
  }

  override def auditDF(df: DataFrame) = {
    df.describe()
  }

  override def processLogs(rdd: RDD[String]) = {

    if (!rdd.partitions.isEmpty) {

      val detailsLogs = rdd.filter(x => x.startsWith("EXECUTION") || x.startsWith("DETAIL")).map(GovernmentUtils.formatDetailsLogs)
      val detailsLogsDF = CoreRepositories.hiveRepository.createDataFrame(detailsLogs, GovernmentStructs.logDetail)

      val executionLogs = rdd.filter(x => x.startsWith("EXECUTION")).map(GovernmentUtils.formatExecutionLogs)
      val executionLogsDF = CoreRepositories.hiveRepository.createDataFrame(executionLogs, GovernmentStructs.logExecutions)

      val auditRaw = rdd.filter(x => x.startsWith("AUDITRAW")).map(GovernmentUtils.formatAuditRaw)
      val auditRawDF = CoreRepositories.hiveRepository.createDataFrame(auditRaw, GovernmentStructs.auditRaw)

      // Guardar en tablas los registros obtenidos del stream
      val auditSchema = CoreConfig.hive.schemas.get(GovernmentConstants.AuditSchema)
      val fullDetailsLogTable = auditSchema + "." + GovernmentConstants.LogDetailsTable
      val fullExecutionLogTable = auditSchema + "." + GovernmentConstants.LogExecutionsTable
      val fullAuditTable = auditSchema + "." + GovernmentConstants.AuditTable

      detailsLogsDF.write.mode(SaveMode.Append).saveAsTable(fullDetailsLogTable)
      executionLogsDF.write.mode(SaveMode.Append).saveAsTable(fullExecutionLogTable)

      auditRawDF.write
        .mode(SaveMode.Append)
        .partitionBy(GovernmentConstants.FechaProceso)
        .saveAsTable(fullAuditTable)

    }

  }

  override def updateMetainfo = {
    println("\n\n" + "Entrando en UpdateMetaInfo")
    println("\n\n" + "Recuperando lastExecutionDate")
    val lastExecutionDateDf = CoreRepositories.hiveRepository.sql(CoreConstants.HqlGovernmentMetainfoGetLastExecutionDate, CoreAppInfo.name, "OK")
    var lastExecutionDate: Option[String] = None
    lastExecutionDateDf match {
      case Some(lastExecutionDateDf) =>
        var result = lastExecutionDateDf.take(1)(0).getString(0)
        if (result == null) result = GovernmentConstants.FirstTimeDate
        lastExecutionDate = Some(result)
      case None => lastExecutionDate = Some(GovernmentConstants.FirstTimeDate)
    }
    println("\n\n" + "Mostrando lastExecutionDate: " + lastExecutionDate)
    println("\n\n" + "Recuperando cambios en base de datos: ")
    val getChangesDF = CoreRepositories.hiveRepository.sql(CoreConstants.HqlGovernmentMetainfoGetChanges, lastExecutionDate.get)
    println("\n\n" + "Mostrando getChangesDF")
    getChangesDF match {
      case Some(getChangesDF) =>
        val changes = getChangesDF.take(3)
        for (change <- changes) {
          println("\n\n" + "La row actual es: " + change)
          val desObject = change.getString(3)
          val sourceType = if (change.getString(2) == "F") "HDFS" else "HIVE"
          var parentPath = change.getString(4)
          if (parentPath.substring(0, 1) != "/") parentPath = "/" + parentPath
          parentPath = parentPath.replaceAll("/", "\\\\" + "/")
          println("\n\n" + s"Recuperando MetainfoObject con parámetros: $desObject, $sourceType, $parentPath")
          val metainfoObject = CoreRepositories.navigatorRepository.findMetainfoObject(desObject, sourceType, parentPath)
          println("\n\n" + "Mostrando MetainfoObject:" + metainfoObject)
        }
      case None =>
    }

  }

  override def putMetainfoObject(objectId: String, values: scala.collection.immutable.Map[String, String]): Boolean = {
    CoreRepositories.navigatorRepository.putMetainfoObject(objectId, values)
  }

  override def recreateSolrDictionaryIndex(name: String): Unit = {
    // Borramos la colección
    CoreRepositories.solrRepository.deleteCollection(name)

    // La creamos con el mismo nombre
    CoreRepositories.solrRepository.createCollection(name)

    // Actualizamos con el estado actual del diccionario
    CoreRepositories.solrRepository.loadDictionaryToCollection(name)
  }
}
