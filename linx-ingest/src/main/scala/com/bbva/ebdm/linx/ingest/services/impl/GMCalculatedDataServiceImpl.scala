package com.bbva.ebdm.linx.ingest.services.impl

import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.Date

import scala.collection.mutable.HashMap

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions._

import com.bbva.ebdm.linx.core.conf.CoreConfig
import com.bbva.ebdm.linx.core.conf.CoreContext
import com.bbva.ebdm.linx.core.conf.CoreRepositories
import com.bbva.ebdm.linx.core.conf.CoreServices
import com.bbva.ebdm.linx.core.exceptions.FatalException
import com.bbva.ebdm.linx.ingest.apps.GMCalculatedDataAppConstants
import com.bbva.ebdm.linx.ingest.services.GMCalculatedDataService

object GMCalculatedDataConstants {
  val Backslash = "/"
  val SourceName = "source_name"
  val SourceOffset = "source_offset"
  val DeltaIrValues = "maturity_deltair"
  val HorizonDate = "horizondate"
  val VolTypeVegainfl = "IV"
  val VolTypeVegaCredito = "CR"
  val VolGroupCF = "CF"
  val VolGroupSwapt = "SWAPT"
  val DeltaFxStd = "Std"
  val DeltaFxTotal = "Total"
  val DeltaFxDummyValue = "dummy"
  val LogLevel = "log_level"
  val LogLevelError = "ERROR"
  val IndAdj = "ind_adj"
  val ApiLogMasterTable = "t_gm_risks_log_api"
}

class GMCalculatedDataServiceImpl extends GMCalculatedDataService {

  def loadHiveTable(srcSchema: String, srcTable: String): DataFrame = {
    val dataFrameRaw = CoreRepositories.hiveRepository.table(srcSchema, srcTable)
    dataFrameRaw
  }

  def loadHiveTablePartition(srcSchema: String, srcTable: String, planDate: Date): DataFrame = {
    
    val dateArray = dateToPartitionFields(planDate)
    val day = "day=" + dateArray(2).toInt
    val month = "month=" + dateArray(1).toInt
    val year = "year=" + dateArray(0).toInt
    val last_version = "last_version=1"
    val where = year + " and " + month + " and " + day + " and " + last_version
    
    val partitionDF = CoreRepositories.hiveRepository
      .sql("/hql/gmcalculateddata/load_table_data.hql", srcSchema, srcTable, where)

    partitionDF match {
      case Some(partitionDF) => partitionDF
      case None => throw new FatalException("No se recuperan valores de " + srcSchema + "." + srcTable)
    }
  }
  
  def loadHiveMasterTablePartition(srcSchema: String, srcTable: String, planDate: Date): DataFrame = {
    
    val dataDate = getPreviousWorkingDay(planDate)
    val dateArray = dateToPartitionFields(dataDate)
    val day = "day=" + dateArray(2).toInt
    val month = "month=" + dateArray(1).toInt
    val year = "year=" + dateArray(0).toInt
    val where = year + " and " + month + " and " + day
    
    val partitionDF = CoreRepositories.hiveRepository
      .sql("/hql/gmcalculateddata/load_table_data.hql", srcSchema, srcTable, where)

    partitionDF match {
      case Some(partitionDF) => partitionDF
      case None => throw new FatalException("No se recuperan valores de " + srcSchema + "." + srcTable)
    }
  }

  def dropAuditRawFields(rddRaw: RDD[Row]): RDD[Row] = {
    val rddWithOutPartition = rddRaw.map { row =>
      var sequence = row.toSeq
      // Quitamos los ultimos campos: created_date, year, month, day y last_version
      sequence.dropRight(5)
    }.map(p => Row.fromSeq(p))

    rddWithOutPartition
  }

  def dropSourceRawFields(rddRaw: RDD[Row]): RDD[Row] = {
    val rddWithOutSources = rddRaw.map { row =>
      var sequence = row.toSeq
      // Quitamos los primeros dos campos: source_name y source_offset
      sequence.drop(2)
    }.map(p => Row.fromSeq(p))

    rddWithOutSources

  }

  def commonInsertTransformations(rddTable: RDD[Row]): RDD[Row] = {
    /*
     * Añadimos y eliminamos las columnas al RDD
     */

    CoreContext.logger.info("Aplicando transformaciones comunes.")
    val geo = "Europe"
    val indAdj = 0
    val rddTrans = rddTable.map { row =>
      var sequence = row.toSeq
      val fecha = sequence(sequence.length - 1).toString().split(GMCalculatedDataConstants.Backslash)
      val partitionDay = fecha(0).toInt
      val partitionMonth = fecha(1).toInt
      val partitionYear = fecha(2).toInt

      sequence.+:(geo).:+(indAdj)
        .:+(partitionYear).:+(partitionMonth).:+(partitionDay)
    }.map(p => Row.fromSeq(p))

    rddTrans
  }

  def insertHorizonDate(rddRawTable: RDD[Row]): RDD[Row] = {
    val rddHorizonDate = rddRawTable.map { row =>
      var sequence = row.toSeq
      val arrayString = sequence(0).toString().split("\\.").apply(0).split("_")
      val fullDate = arrayString(arrayString.length - 1)
      // Hacemos substring por valores absolutos porque no sabemos si la fecha va a venir de la manera YYYYMMDD o YYYYMMDDhhmm
      val day = fullDate.substring(6, 8)
      val month = fullDate.substring(4, 6)
      val year = fullDate.substring(0, 4)
      val horizonDate = day + GMCalculatedDataConstants.Backslash + month + GMCalculatedDataConstants.Backslash + year
      sequence.:+(horizonDate)
    }.map(p => Row.fromSeq(p))

    rddHorizonDate
  }

  def dv01nbTransformations(rddRawTable: RDD[Row]): RDD[Row] = {
    // Cargamos un Array con las lineas que contienen las fechas de Maturity
    val headerArray = rddRawTable.filter(x => x.getAs[Long](GMCalculatedDataConstants.SourceOffset) == 1).collect()
    // Cargamos un Map con el nombre del fichero como clave y un array de fechas como valor
    var headerMap: HashMap[String, Array[String]] = new HashMap
    headerArray.foreach { x =>
      headerMap.put(x.getAs[String](GMCalculatedDataConstants.SourceName), 
                    x.getAs[String](GMCalculatedDataConstants.DeltaIrValues).split(";"))
    }
    // Distribuimos el map a todos los nodos
    val headerBrcst = CoreContext.sc.broadcast(headerMap)

    // Creamos el DataFrame con los datos
    val dataRdd = rddRawTable.filter(x => x.getAs(GMCalculatedDataConstants.SourceOffset) != 1)

    // Generamos las filas en el formato deseado.
    // Usamos "flatMap" ya que de cada fila de entrada generamos varias de salida
    val finalRdd = dataRdd.flatMap { row =>
      // Constantes
      val geo = "Europe"
      val indAdj = 0
      // Array con los valores de Delta IR
      val matValues = row.getAs[String](GMCalculatedDataConstants.DeltaIrValues).split(";")
      // Array con los valores de Maturity
      val matDates = headerBrcst.value.get(row.getAs[String](GMCalculatedDataConstants.SourceName)).get
      // Obtenemos las claves de particionamiento del Horizon Date
      val horizondate = row.getAs[String](GMCalculatedDataConstants.HorizonDate).split(GMCalculatedDataConstants.Backslash)
      val partitionDay = horizondate(0).toInt
      val partitionMonth = horizondate(1).toInt
      val partitionYear = horizondate(2).toInt
      var output: Array[Row] = Array()
      for (i <- 0 to matValues.length - 1) {
        if (matValues(i) != "") {
          output = output.:+(Row.fromSeq(Seq(
            geo,
            row.getAs[String](2),
            row.getAs[String](3),
            row.getAs[String](4),
            matDates(i),
            matValues(i).toDouble,
            row.getAs[String](GMCalculatedDataConstants.HorizonDate),
            indAdj,
            partitionYear,
            partitionMonth,
            partitionDay)))
        }
      }
      output
    }
    finalRdd
  }

  def expFxTransformations(cleanRdd: RDD[Row]): RDD[Row] = {
    //Uso de flatmap para verticalizar registros correspondientes a las sensibilidades ExpFx_pl y ExpFx_opt
    val rddExpFx = cleanRdd.flatMap { row =>
      //Vamos a crear un array con los dos elementos que va a verticalizar la funcion flatmap
      val SeqExpFxPl = Seq(row.getAs[String](0), row.getAs[String](1), row.getAs[String](2), row.getAs[String](3),
        row.getAs[String](4), row.getAs[String](5), row.getAs[String](6), row.getAs[String](7),
        row.getAs[Double](8), row.getAs[String](11))
      val RowExpFxPl = Row.fromSeq(SeqExpFxPl)
      val SeqExpFxOpt = Seq(row.getAs[String](0), row.getAs[String](1), row.getAs[String](2), row.getAs[String](3),
        row.getAs[String](4), row.getAs[String](5), row.getAs[String](6), row.getAs[String](9),
        row.getAs[Double](10), row.getAs[String](11))
      val RowExpFxOpt = Row.fromSeq(SeqExpFxOpt)
      Array[Row](RowExpFxPl, RowExpFxOpt)
    }
    val rowsNotEmpty = rddExpFx.filter { row => !row.getAs[String](7).isEmpty() }
    val groupedRows = rowsNotEmpty.groupBy { row =>
      Seq(row.getAs[String](0), row.getAs[String](1), row.getAs[String](2),
        row.getAs[String](3), row.getAs[String](4), row.getAs[String](5),
        row.getAs[String](6), row.getAs[String](7), row.getAs[String](9))
    }
    val sumGrouppedRows = groupedRows.map { row =>
      var sum: Double = 0
      row._2.map { rowIt =>
        sum = sum + rowIt.getAs[Double](8)
      }
      val horizondate = row._1(8)
      Row.fromSeq(row._1.dropRight(1).:+(sum).:+(horizondate))
    }
    sumGrouppedRows
  }

  def vegaDivisionInflacion(cleanRdd: RDD[Row]): RDD[Row] = {
    //Dividir todos los rDD, con filter!!!

    val rddVegaInf = cleanRdd.filter { row =>
      val seq = row.toSeq
      val volType = seq(1).toString()
      volType.contains(GMCalculatedDataConstants.VolTypeVegainfl)
    }
    rddVegaInf
  }

  def vegaDivisionCredito(cleanRdd: RDD[Row]): RDD[Row] = {
    //Dividir todos los rDD, con filter!!!

    val rddVegaCrd = cleanRdd.filter { row =>
      val seq = row.toSeq
      val volType = seq(1).toString()
      volType.contains(GMCalculatedDataConstants.VolTypeVegaCredito)
    }

    rddVegaCrd
  }

  def filterVegaDivisions(cleanRdd: RDD[Row]): RDD[Row] = {

    val rddOnlyVegaIR = cleanRdd

      .filter { row =>
        val seq = row.toSeq
        val volType = seq(1).toString()
        !volType.contains(GMCalculatedDataConstants.VolTypeVegainfl)
      }

      .filter { row =>
        val seq = row.toSeq
        val volType = seq(1).toString()
        !volType.contains(GMCalculatedDataConstants.VolTypeVegaCredito)
      }

    rddOnlyVegaIR

  }

  def selectGroupVegaInfl(cleanRdd: RDD[Row]): RDD[Row] = {

    val rddVegaInflFromVegaIr = cleanRdd.map { row =>
      // portfolio,vol_type,volgroup,instrument,maturity,optmaturity,nb,vegayield,horizondate
      val sequence = row.toSeq

      var group = GMCalculatedDataConstants.VolGroupSwapt
      // Si el campo Vol. group contiene "CF" informamos el campo group como "CF"
      if (sequence(2).toString().contains(GMCalculatedDataConstants.VolGroupCF)) {
        group = GMCalculatedDataConstants.VolGroupCF
      }

      val newInstrument = sequence(3)
      val newvolGroup = sequence(2)

      // portfolio,group,instrument,volgroup,maturity,optmaturity,nb,vegainfl,horizondate
      val newRow = Row.fromSeq(Seq(sequence(0), group, newInstrument, newvolGroup, sequence(4), sequence(5), sequence(6), sequence(7), sequence(8)))
      newRow
    }

    rddVegaInflFromVegaIr
  }

  def filterRowsDeltafx(rddRawTable: RDD[Row]): RDD[Row] = {
    val groupByKeyRdd = rddRawTable.map(x => (x.getAs[String](GMCalculatedDataConstants.SourceName), x)).groupByKey()
    val cuttedRdd = groupByKeyRdd.flatMap {
      case (sourcename, file) =>
        val offsetStd = file.filter(x => x.getAs[String](2).contains(GMCalculatedDataConstants.DeltaFxStd))
          .toSeq.apply(0).getAs[Long](1)
        val offsetTotal = file.filter(x => x.getAs[String](2).contains(GMCalculatedDataConstants.DeltaFxTotal))
          .toSeq.apply(0).getAs[Long](1)
        val finalArray = file.filter(x => x.getAs[Long](1) > offsetStd && x.getAs[Long](1) < offsetTotal)
        finalArray
    }

    cuttedRdd
  }

  def transformationsDeltafx(cleanRdd: RDD[Row]): RDD[Row] = {

    /* 
     * Para el primer valor de currency nos quedamos con el segundo valor que viene en el par, 
     * p.e: Si vieniera algo asi EUR-USD , nos quedamos con USD
     */
    val groupByKeyRdd = cleanRdd.map(x => (x.getAs[String](GMCalculatedDataConstants.SourceName), x)).groupByKey()
    val finalRdd = groupByKeyRdd.flatMap { x =>
      val sortedList = x._2.toList.sortBy(_.getAs[Long](1))
      var currency = (sortedList(0).getAs[String](2).split("-"))(1)
      val regs = sortedList.map { row =>
        if (!row.getAs[String](2).trim().isEmpty() && !row.getAs[String](2).contains("-")) {
          currency = row.getAs[String](2).trim()
        }
        val groupAndPortfolio = row.getAs[String](3).split("\\+")
        var group = ""
        var portfolio = ""
        if (groupAndPortfolio.length == 2) {
          group = groupAndPortfolio(0)
          portfolio = groupAndPortfolio(1)
        }

        val newRow = Row.fromSeq(Seq(row.getAs[String](0), row.getAs[Long](1), currency, group, portfolio, row.getAs[Double](4), row.getAs[String](5), row.getAs[Double](6)))
        newRow
      }
      regs
    }
    finalRdd
  }

  def createTableDataFrame(rdd: RDD[Row], tgtSchema: String, tgtTable: String): DataFrame = {
    val struct = CoreServices.governmentService.getTableDefinition(tgtSchema, tgtTable)
    val df = CoreRepositories.hiveRepository.createDataFrame(rdd, struct)
    df
  }

  def saveMasterTable(DFToWrite: DataFrame, tgtSchema: String, tgtTable: String): DataFrame = {
    val fullTableName = CoreConfig.hive.schemas.get(tgtSchema) + "." + tgtTable
    DFToWrite.repartition(8)
      .write
      .mode(SaveMode.Append)
      .partitionBy("year", "month", "day")
      .saveAsTable(fullTableName)
    DFToWrite
  }

  def apiLogErrorFilter(cleanRdd: RDD[Row]): RDD[Row] = {
    val errorRdd = cleanRdd.filter { x =>
       x.getAs[String](5).contains(GMCalculatedDataConstants.LogLevelError)
    }
    errorRdd
  }

  def apiLogReplaceDate(cleanRdd: RDD[Row], planDate: Date): RDD[Row] = {
    val prevWorkDay = getPreviousWorkingDay(planDate)
    val completePrevWrkDay = dateToPartitionFields(prevWorkDay)
    val year = completePrevWrkDay(0).toInt
    val month = completePrevWrkDay(1).toInt
    val day = completePrevWrkDay(2).toInt
    val addWrkDayPartition = cleanRdd.map { row =>
      var sequence = row.toSeq
      sequence.:+(year).:+(month).:+(day)
    }.map(p => Row.fromSeq(p))

    addWrkDayPartition
  }

  def distinctNBApiLog(apiLogDF: DataFrame): DataFrame = {
    import CoreContext.sqlContext.implicits._
    val badNBApiLogDF = apiLogDF.map(_.getAs[String](6)).toDF("nb").distinct()
    badNBApiLogDF
  }

  def applyApilog(badNbDF: DataFrame, table: String, planDate: Date): DataFrame = {

    val todayDate = planDate
    val yesterdayDate = getPreviousWorkingDay(planDate)

    val struct = CoreServices.governmentService.getTableDefinition(GMCalculatedDataAppConstants.tgtSchema, table)
    val todayHorizondate = "'" + dateToHorizondate(todayDate) + "' as horizondate"
    val todayIndAdj = "1 as ind_adj"
    //val todayIndAdj = "decode(ind_adj,0,1,2) as ind_adj"
    val partitionDate = dateToPartitionString(todayDate)
    val fields = struct.fieldNames.mkString(",")
      .replace("horizondate", todayHorizondate)
      .replace("ind_adj", todayIndAdj)
      .replace("year,month,day", partitionDate)

    /* Parametros de la query para aplicar Api Log
     * 1 - Esquema de master
     * 2 - Tabla a aplicar Api Log
     * 3 - Tabla Api Log
     * 4 - Year
     * 5 - Month
     * 6 - Dia actual
     * 7 - Dia anterior
     * 8 - Lista de campos de la tabla a aplicar Api Log 
     */
    val applyApiLogDF = CoreRepositories.hiveRepository.sql("/hql/gmcalculateddata/apply_api_log.hql",
      GMCalculatedDataAppConstants.tgtSchema,
      table,
      GMCalculatedDataConstants.ApiLogMasterTable,
      dateToPartitionFields(todayDate)(0),
      dateToPartitionFields(todayDate)(1),
      dateToPartitionFields(todayDate)(2),
      dateToPartitionFields(yesterdayDate)(2),
      fields)

    val fullOkData = applyApiLogDF match {
      case Some(applyApiLogDF) => applyApiLogDF
      case None => throw new FatalException("Error ejecutando al query para aplicar Api Log en la tabla " + GMCalculatedDataAppConstants.tgtSchema + "." + table)
    }

    fullOkData
  }

  def apiLogReplaceMasterPartition(DFToWrite: DataFrame, tgtSchema: String, tgtTable: String):DataFrame ={
    if (!CoreServices.commonService.isEmptyDF(DFToWrite)) {
      CoreRepositories.hiveRepository.refreshTable(tgtSchema, tgtTable)
      val keys = DFToWrite.select("source_name", "year", "month", "day").distinct()
      val whereConds = getWherePartitions(keys.collect())
      val oldMasterDF = CoreRepositories.hiveRepository.sql("/hql/gmcalculateddata/load_table_data.hql",
        tgtSchema,
        tgtTable,
        whereConds).get
                               
      val oldFiles = oldMasterDF
        .select(input_file_name())
        .distinct()
        .collect()

      val oldOkDF = oldMasterDF.alias("old")
        .join(keys.alias("keys"), 
                   oldMasterDF.col("source_name") === keys.col("source_name"), 
                   "left_outer")
        .where("keys.year is null")
        .select("old.*")
      val mergedDF = DFToWrite.unionAll(oldOkDF)
      
      saveMasterTable(mergedDF, tgtSchema, tgtTable)
      oldFiles.foreach { x =>
        val inputFileName = x.getAs[String](0)
        CoreRepositories.dfsRepository.deleteAbsolutePath(inputFileName)
      }
      CoreRepositories.impalaRepository.invalidate(tgtSchema, tgtTable)
    }
    DFToWrite
  }

  def replaceMasterPartition(DFToWrite: DataFrame, tgtSchema: String, tgtTable: String): DataFrame = {
    if (!CoreServices.commonService.isEmptyDF(DFToWrite)) {
      CoreRepositories.hiveRepository.refreshTable(tgtSchema, tgtTable)
      val keys = DFToWrite.select("portfolio", "horizondate", "year", "month", "day").distinct()
      val whereConds = getWherePartitions(keys.collect())
      val oldMasterDF = CoreRepositories.hiveRepository.sql("/hql/gmcalculateddata/load_table_data.hql",
        tgtSchema,
        tgtTable,
        whereConds).get
                               
      val oldFiles = oldMasterDF
        .select(input_file_name())
        .distinct()
        .collect()

      val oldOkDF = oldMasterDF.alias("old")
        .join(keys.alias("keys"), 
                   oldMasterDF.col("portfolio") === keys.col("portfolio") && oldMasterDF.col("horizondate") === keys.col("horizondate"), 
                   "left_outer")
        .where("keys.year is null")
        .select("old.*")
      val mergedDF = DFToWrite.unionAll(oldOkDF)
      
      saveMasterTable(mergedDF, tgtSchema, tgtTable)
      oldFiles.foreach { x =>
        val inputFileName = x.getAs[String](0)
        CoreRepositories.dfsRepository.deleteAbsolutePath(inputFileName)
      }
      CoreRepositories.impalaRepository.invalidate(tgtSchema, tgtTable)
    }
    DFToWrite
  }

  private def getCalendarFromDate(planDate: Date): Calendar = {
    val cal = Calendar.getInstance
    cal.setTime(planDate)
    cal
  }

  private def addDate(planDate: Date, days: Int): Date = {
    val cal = Calendar.getInstance
    cal.setTime(planDate)
    cal.add(Calendar.DAY_OF_MONTH, days)
    cal.getTime
  }

  private def dateToHorizondate(date: Date): String = {
    val formatter: DateFormat = new SimpleDateFormat("yyyy/MM/dd")
    formatter.format(date)
  }

  private def dateToPartitionString(date: Date): String = {
    val formatter: DateFormat = new SimpleDateFormat("yyyy, MM, dd")
    formatter.format(date)
  }

  private def dateToPartitionFields(date: Date): Array[String] = {
    val formatter: DateFormat = new SimpleDateFormat("yyyy-MM-dd")
    formatter.format(date).split("-")
  }

  private def getPreviousWorkingDay(date: Date): Date = {
    val cal = getCalendarFromDate(date)
    var dayOfWeek: Int = 0
    do {
      cal.add(Calendar.DAY_OF_MONTH, -1)
      dayOfWeek = cal.get(Calendar.DAY_OF_WEEK)
    } while (dayOfWeek == Calendar.SATURDAY || dayOfWeek == Calendar.SUNDAY)

    cal.getTime();
  }
  
  private def getWherePartitions(dates:Array[Row]):String = {
    val conds = dates.map{row => 
      "(" + 
      Seq(
        "year=" + row.getAs[Int]("year"),
        "month=" + row.getAs[Int]("month"),
        "day=" + row.getAs[Int]("day")
        ).mkString(" and ") + ")"
    }
    conds.distinct.mkString(" or ")
  }

}
