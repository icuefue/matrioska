package com.bluetab.matrioska.ingest.services.impl

import java.io.ByteArrayInputStream
import java.io.InputStreamReader
import java.nio.file.Paths
import java.text.SimpleDateFormat
import java.util.Date

import scala.collection.mutable.ListBuffer
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.input_file_name
import org.apache.spark.sql.functions.when
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.types.DecimalType
import org.apache.spark.sql.types.StructType
import com.bluetab.matrioska.core.beans.MailMessage
import com.bluetab.matrioska.core.beans.MailRecipientTypes
import com.bluetab.matrioska.core.conf.CoreContext
import com.bluetab.matrioska.core.conf.CoreRepositories
import com.bluetab.matrioska.core.conf.CoreServices
import com.bluetab.matrioska.core.exceptions.PathIsNotEmptyDirectoryException
import com.bluetab.matrioska.ingest.beans.DelimitedFileType
import com.bluetab.matrioska.ingest.beans.FileType
import com.bluetab.matrioska.ingest.beans.FixedLengthFileType
import com.bluetab.matrioska.ingest.beans.Mask
import com.bluetab.matrioska.ingest.beans.SqoopCsvFileType
import com.bluetab.matrioska.ingest.constants.IngestDfsConstants
import au.com.bytecode.opencsv.CSVReader
import com.bluetab.matrioska.ingest.beans.AllInOneFieldFileType
import com.bluetab.matrioska.core.beans.{Attachment, MailMessage, MailRecipientTypes}
import com.bluetab.matrioska.core.conf.{CoreConfig, CoreRepositories, CoreServices}
import com.bluetab.matrioska.ingest.beans.AuditFileData
import com.bluetab.matrioska.ingest.constants.{IngestConstants, IngestDfsConstants}
import com.bluetab.matrioska.ingest.services.PrerawToRawService
import com.bluetab.matrioska.ingest.utils.FaultToleranceUtils

object LineStatus extends Enumeration {
  val Ok, NumColumnsError, CastError, LineMatchExpressionError = Value
}

object PrerawToRawConstants {
  val YearField = "year"
  val MonthField = "month"
  val DayField = "day"
  val SourceNameField = "source_name"
  val LastVersionField = "last_version"
  val SourceOffsetField = "source_offset"
  val CreatedDateField = "created_date"
  val sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS")

  def getYearMonthDay(file: String): (String, String, String) = {
    val fragments = file.split("/")
    val year = fragments(2).substring(PrerawToRawConstants.YearField.length + 1)
    val month = fragments(3).substring(PrerawToRawConstants.MonthField.length + 1)
    val day = fragments(4).substring(PrerawToRawConstants.DayField.length + 1)
    (year, month, day)
  }
}

object PrerawToRawUtils {
  def castFields(fields: Tuple2[Seq[String], Int], structType: StructType): Tuple2[Seq[Any], Int] = {
    var i = 0
    val result = new Array[Any](structType.size)
    if (fields._1.length != structType.size) {
      (fields._1, LineStatus.NumColumnsError.id)
    } else {
      try {
        for (structField <- structType) {
          structField.dataType match {
            case DataTypes.DoubleType =>
              result(i) = if (fields._1(i).toString().isEmpty() || fields._1(i).toString() == "null") null else fields._1(i).toDouble
            case DataTypes.IntegerType =>
              result(i) = if (fields._1(i).toString().isEmpty() || fields._1(i).toString() == "null") null else fields._1(i).toInt
            case DataTypes.LongType =>
              result(i) = if (fields._1(i).toString().isEmpty() || fields._1(i).toString() == "null") null else fields._1(i).toLong
            case DataTypes.ShortType =>
              result(i) = if (fields._1(i).toString().isEmpty() || fields._1(i).toString() == "null") null else fields._1(i).toShort
            case DataTypes.BooleanType =>
              result(i) = if (fields._1(i).toString().isEmpty() || fields._1(i).toString() == "null") null else fields._1(i).toBoolean
            case DataTypes.DateType =>
              result(i) = if (fields._1(i).toString().isEmpty() || fields._1(i).toString() == "null") null else PrerawToRawConstants.sdf.parse(fields._1(i))
            case DecimalType() =>
              result(i) = if (fields._1(i).toString().isEmpty() || fields._1(i).toString() == "null") null else new java.math.BigDecimal(fields._1(i))
            case _ => result(i) = fields._1(i)
          }
          i = i + 1
        }
      } catch {
        case t: Throwable =>
          (fields._1, LineStatus.CastError.id)
      }
      (result, fields._2)
    }
  }

  def addExtraFields(fields: Seq[String], offset: Long, errorCode: Int, filename: String, createdDate: String): Tuple2[Seq[String], Int] = {
    val (year, month, day) = PrerawToRawConstants.getYearMonthDay(filename)
    val lastVersion = "1"
    (filename +: offset.toString +: fields :+ createdDate :+ year :+ month :+ day :+ lastVersion, errorCode)
  }

  def matchPattern(line: String, offset: Long, linePattern: String): Tuple3[Seq[String], Long, Int] = {
    val pattern = linePattern.r
    val fields = pattern.findFirstMatchIn(line)
    fields match {
      case Some(fields) =>
        var result = Seq[String]()
        for (i <- 1 to fields.groupCount) {
          result = result :+ fields.group(i)
        }
        // Metemos en el segundo campo  0 para indicar que la línea es correcta
        (result, offset, LineStatus.Ok.id)
      case None =>
        // Metemos en el segundo campo - 1 para indicar que la línea está mal
        (Seq(), offset, LineStatus.LineMatchExpressionError.id)
    }
  }
}

class PrerawToRawServiceImpl extends PrerawToRawService {

  def getToLoadFilenames: Seq[String] = {
    val filenames = CoreRepositories.dfsRepository.listFiles(IngestDfsConstants.PrerawToLoad, true)
    val result = new ListBuffer[String]()
    filenames.foreach { filename =>
      //Esto filtra los ficheros temporales creados por SQOOP
      if (!filename.matches("(.*)/_temporary/(.*)")) {
        val aux = filename.replaceFirst("^" + IngestDfsConstants.PrerawToLoad, "")
        result += aux
      }
    }
    result
  }

  private def renameFile(source: String, destination: String, filename: String) {

    val sourceFilename = source + filename
    val destinationFilename = destination + filename

    val p = Paths.get(filename)
    val directory = p.getParent.toString
    val destinationDirectory = destination + directory
    if (!CoreRepositories.dfsRepository.exists(destinationDirectory)) {
      CoreRepositories.dfsRepository.mkdirs(destinationDirectory)
    }
    if (CoreRepositories.dfsRepository.exists(destinationFilename))
      CoreRepositories.dfsRepository.delete(destinationFilename)
    CoreRepositories.dfsRepository.rename(sourceFilename, destinationFilename)
  }

  def markFilesAsInProgress(filenames: Seq[String]) = {

    for (filename <- filenames) markFileAsInProgress(filename)

  }

  def markFileAsInProgress(filename: String) = {

    renameFile(IngestDfsConstants.PrerawToLoad, IngestDfsConstants.PrerawInProgress, filename)

  }

  def markAuditFilesAsFailure(files: Seq[AuditFileData]) = {

    for (file <- files) markFileAsFailure(file.path)

  }

  def markFilesAsFailure(filenames: Seq[String]) = {

    for (filename <- filenames) markFileAsFailure(filename)

  }

  def markFileAsFailure(filename: String) = {

    renameFile(IngestDfsConstants.PrerawInProgress, IngestDfsConstants.PrerawFailures, filename)

  }

  def markFilesAsSuccessful(filenames: Seq[String]) = {

    for (filename <- filenames) markFileAsSuccessful(filename)

  }

  def markFileAsSuccessful(filename: String) = {

    renameFile(IngestDfsConstants.PrerawInProgress, IngestDfsConstants.PrerawSuccessful, filename)

  }

  def loadFile(filename: String, mask: Mask, tableStruct: StructType): RDD[Tuple2[Seq[Any], Int]] = {

    loadFile(mask.fileType, filename, tableStruct)

  }

  private def loadFile(fileType: FileType, filename: String, tableStruct: StructType): RDD[Tuple2[Seq[Any], Int]] = {
    //Eliminamos la primera columna que es el nombre del fichero
    fileType match {
      case a: DelimitedFileType => loadDelimitedFile(a, filename, tableStruct)
      case b: FixedLengthFileType => loadFixedLenghtFile(b, filename, tableStruct)
      case c: SqoopCsvFileType => loadSqoopCsvFile(c, filename, tableStruct)
      case d: AllInOneFieldFileType => loadAllInOneFieldFile(d, filename, tableStruct)
    }
  }

  /**
   * Carga en RDD de un fichero delimitado
   *
   * @param fileType - propiedades del fichero delimitado
   * @param filename - nombre del fichero a cargar
   * @param tableStruct - estructura de la tabla a la que se va a volcar el fichero
   * @return - RDD con el contenido de las tuplas del fichero
   */
  private def loadDelimitedFile(fileType: DelimitedFileType, filename: String, tableStruct: StructType): RDD[Tuple2[Seq[Any], Int]] = {
    val path = IngestDfsConstants.PrerawInProgress + filename
    val zippedWithIndexFile = CoreRepositories.dfsRepository.textFile(path).zipWithIndex()
    val createdDate = PrerawToRawConstants.sdf.format(new Date)
    zippedWithIndexFile
      .filter(x => x._2 >= fileType.header)
      .filter(x => x._1 != "\u001a")
      .map { x =>
        val line = x._1
        var fields = line.split(fileType.fieldDelimiter, -1)
        if (fileType.endsWithDelimiter) {
          fields = line.split(fileType.fieldDelimiter)
        }
        PrerawToRawUtils.addExtraFields(fields.toSeq, x._2, 0, filename, createdDate)
      }
      .map(p => PrerawToRawUtils.castFields(p, tableStruct))
  }

  /**
   * Carga en RDD de un fichero CSV traído con Sqoop
   *
   * @param fileType - propiedades del fichero CSV traído con Sqoop
   * @param filename - nombre del fichero a cargar
   * @param tableStruct - estructura de la tabla a la que se va a volcar el fichero
   * @return - RDD con el contenido de las tuplas del fichero
   */
  def loadSqoopCsvFile(fileType: SqoopCsvFileType, filename: String, tableStruct: StructType): RDD[(Seq[Any], Int)] = {
    val path = IngestDfsConstants.PrerawInProgress + filename
    val zippedWithIndexFile = CoreRepositories.dfsRepository.textFile(path).zipWithIndex()
    val createdDate = PrerawToRawConstants.sdf.format(new Date)
    zippedWithIndexFile
      .filter(x => x._2 >= fileType.header)
      .map { x =>
        val line = x._1

        val isr = new InputStreamReader(new ByteArrayInputStream(line.getBytes()))
        val csvReader = new CSVReader(isr, fileType.fieldDelimiter.charAt(0), fileType.enclosedBy.charAt(0), fileType.escapedBy.charAt(0))
        val fields = csvReader.readAll().get(0)

        PrerawToRawUtils.addExtraFields(fields.toSeq, x._2, 0, filename, createdDate)
      }
      .map(p => PrerawToRawUtils.castFields(p, tableStruct))
  }

  /**
   * Carga en RDD de un fichero de ancho fijo
   *
   * @param fileType - propiedades del fichero de ancho fijo
   * @param filename - nombre del fichero a cargar
   * @param tableStruct - estructura de la tabla a la que se va a volcar el fichero
   * @return - RDD con el contenido de las tuplas del fichero
   */
  private def loadFixedLenghtFile(fileType: FixedLengthFileType, filename: String, tableStruct: StructType): RDD[Tuple2[Seq[Any], Int]] = {
    val path = IngestDfsConstants.PrerawInProgress + filename
    val createdDate = PrerawToRawConstants.sdf.format(new Date)
    CoreRepositories.dfsRepository.textFile(path).zipWithIndex()
      .filter(x => x._2 >= fileType.header)
      .filter(x => x._1 != "\u001a")
      .map(x => PrerawToRawUtils.matchPattern(x._1, x._2, fileType.linePattern))
      .map(x => PrerawToRawUtils.addExtraFields(x._1, x._2, x._3, filename, createdDate))
      .map(p => PrerawToRawUtils.castFields(p, tableStruct))
  }
  
  /**
   * Carga en RDD de un fichero con todo su contenido en un solo campo
   *
   * @param fileType - propiedades del fichero
   * @param filename - nombre del fichero a cargar
   * @param tableStruct - estructura de la tabla a la que se va a volcar el fichero
   * @return - RDD con el contenido de las tuplas del fichero
   */
  private def loadAllInOneFieldFile(fileType: AllInOneFieldFileType, filename: String, tableStruct: StructType): RDD[Tuple2[Seq[Any], Int]] = {
    val path = IngestDfsConstants.PrerawInProgress + filename
    val createdDate = PrerawToRawConstants.sdf.format(new Date)
    CoreRepositories.dfsRepository.wholeTextFile(path)
      .map(x => x._2)
      .map(x => PrerawToRawUtils.addExtraFields(Seq(x) , 1, 0, filename, createdDate))
      .map(p => PrerawToRawUtils.castFields(p, tableStruct))
  }

  def checkQuality(df: DataFrame, filename: String) = {

    val dfStats = CoreServices.auditoryService.auditDF(filename, df)
    CoreServices.auditoryService.saveAsFileStats(dfStats)

  }

  def saveToTable(rdd: RDD[Tuple2[Seq[Any], Int]], mask: Mask, tableStruct: StructType, filenames: Seq[String]): (Seq[String], Seq[AuditFileData]) = {

    rdd.cache()
    // Auditamos los ficheros lo que nos devuelve el conjunto de ficheros erroneos
    val badFiles = auditRdd(rdd, mask)

    var goodFiles = Seq[String]()
    filenames.foreach { filename =>
      if (!badFiles.contains(filename)) goodFiles = goodFiles :+ filename
    }

    //Filtramos los ficheros erroneos de lo que vamos a cargar en Tabla y también los registros erroneos
    val rddFinal = filterBadFiles(rdd, badFiles).filter(x => x._2 == 0)

    //ACCIONES PARA REPROCESAR
    // Obtengo fechas de las particiones afectadas por el nuevo dataset
    val dates = getDateFiles(goodFiles)

    //Recupero un DataFrame con toda la información de las particiones
    var whereList = Seq[String]()
    dates.foreach(x => whereList = whereList :+ "(year=" + x._1 + " and month=" + x._2 + " and day= " + x._3 + ")")
    val where = if (dates.length > 0) whereList.mkString(" or ") else "1 == 0"
    //Cargamos el contenido a reprocesar que solo será el que tiene last version = 1
    var currentContentDF = CoreRepositories.hiveRepository.sql(IngestConstants.HqlPrerawToRawSelectForReprocess,
      mask.table.schema, mask.table.table, where).get
    //Recupero un listado con todos los ficheros fisicos de la información actual
    val oldFiles = currentContentDF.select(input_file_name()).distinct().collect()
    currentContentDF = currentContentDF.withColumn(PrerawToRawConstants.LastVersionField, when((col(PrerawToRawConstants.SourceNameField).isin(goodFiles: _*)), 0).otherwise(1))

    val newContentDF = CoreContext.hiveContext.createDataFrame(rddFinal.map(p => Row.fromSeq(p._1)), tableStruct)

    // Ya no es necesario hacer el filter puesto que no se elimina nada
    // filter(!(col(PrerawToRawConstants.SourceNameField).isin(goodFiles: _*)))
    // Se une el contenido de lo antiguo con lo nuevo
    var finalContentDF = currentContentDF.unionAll(newContentDF)

    CoreRepositories.hiveRepository.setCompressionCodec(mask.table.compressionCodec)
    // 
    mask.table.defaultPartitionning match {
      case true =>
//        finalContentDF.write.mode(SaveMode.Append)
//          .partitionBy(PrerawToRawConstants.YearField, PrerawToRawConstants.MonthField, PrerawToRawConstants.DayField, PrerawToRawConstants.LastVersionField)
//          .saveAsTable(mask.table.getFullName)
        CoreRepositories.hiveRepository.saveToTable(finalContentDF, mask.table.schema, mask.table.table, Seq(PrerawToRawConstants.YearField, PrerawToRawConstants.MonthField, PrerawToRawConstants.DayField, PrerawToRawConstants.LastVersionField))
      case false =>
//        finalContentDF.write.mode(SaveMode.Append).saveAsTable(mask.table.getFullName)
        CoreRepositories.hiveRepository.saveToTable(finalContentDF, mask.table.schema, mask.table.table)
    }
    //Borro todos los ficheros antiguos 
    oldFiles.foreach { x =>
      val inputFileName = x.getString(0)
      CoreRepositories.dfsRepository.deleteAbsolutePath(inputFileName)
    }

    rdd.unpersist()
    (goodFiles, badFiles)
  }

  def filterBadFiles(rdd: RDD[Tuple2[Seq[Any], Int]], badFiles: Seq[AuditFileData]) = {
    val rddFinal = rdd.filter { x =>
      var resultado = true
      for (badFile <- badFiles) {
        if (badFile.path == x._1(0)) resultado = false
      }
      resultado
    }
    rddFinal
  }

  def deleteDirectories(filenames: Seq[String]) = {
    CoreContext.logger.info("Borrando los directorios vacíos")
    val pathToLoad = IngestDfsConstants.PrerawToLoad
    val pathInProgress = IngestDfsConstants.PrerawInProgress
    var directoriesToDelete = Seq[String]()
    filenames.foreach { filename =>
      var fragments = filename.split("/").dropRight(1)
      var directory = fragments.mkString("/")
      if (!directoriesToDelete.contains(directory)) {
        directoriesToDelete = directoriesToDelete :+ (directory)
      }
      if (!directoriesToDelete.contains(directory)) {
        directoriesToDelete = directoriesToDelete :+ (directory)
      }
      fragments = fragments.dropRight(1)
    }

    directoriesToDelete.foreach { directory =>
      var fragments = directory.split("/")
      while (fragments.length > 1) {
        var directory = fragments.mkString("/")
        deleteDirectory(pathToLoad + directory)
        deleteDirectory(pathInProgress + directory)
        fragments = fragments.dropRight(1)
      }
    }
  }

  private def deleteDirectory(path: String) = {
    try {
      CoreRepositories.dfsRepository.delete(path)
    } catch {
      case e: PathIsNotEmptyDirectoryException => CoreContext.logger.debug("No se ha podido borrar:" + path + ". Directorio no está vacío")
      case t: Throwable => throw t
    }
  }

  def sendErrorNofitication(errorMessage: String) = {
    val subject = if (CoreConfig.environment == "PRO")
      "[PrerawToRaw] Listado de ficheros erróneos"
    else "DESARROLLO [PrerawToRaw]  Listado de ficheros erróneos"

    val mailMessage = new MailMessage
    mailMessage.addRecipient(MailRecipientTypes.To, "soporte_data_analytics_cib@bbva.com")
    mailMessage.subject = subject
    mailMessage.text = "Se han producido errores en la subida de los ficheros adjuntos"
    mailMessage.addAttachment(Attachment("Ficheros_fallidos.txt", errorMessage))

    CoreRepositories.mailRepository.sendMail(mailMessage)
  }

  def checkInProgress: Seq[String] = {
    val filenames = CoreRepositories.dfsRepository.listFiles(IngestDfsConstants.PrerawInProgress, true)
    val result = new ListBuffer[String]()
    filenames.foreach { filename =>
      val aux = filename.replaceFirst("^" + IngestDfsConstants.PrerawInProgress, "")
      result += aux
    }
    result
  }

  def auditRdd(rdd: RDD[Tuple2[Seq[Any], Int]], mask: Mask): Seq[AuditFileData] = {
    // Se obtienen el numero de registros buenos y malos del rdd que vamos a guardar

    val auditRdd = rdd.map(x => {
      val errorCode = x._2
      if (errorCode == 0) {
        (x._1(0).toString(), (1, 1))
      } else {
        (x._1(0).toString(), (0, 1))
      }
    }).reduceByKey((accum, x) => ((accum._1 + x._1), (accum._2 + x._2))).collect
    
    //Los ficheros con lineas erroneas se consideran erroneos y no se cargan.
    //Se envia a auditoría 
    var badFiles = Seq[AuditFileData]()
    auditRdd.foreach(x => {
      if (x._2._1 != x._2._2) {
        
        val fileRdd = rdd.filter(line => x._1.equals(line._1(0)))
        
        if (checkFaultTolerance(mask, fileRdd, x._2._1, x._2._2)) {
          CoreContext.logger.debug("HA PASADO EL TEST DE TOLERANCIA:" + x._1)
          CoreServices.governmentService.auditRawTable(x._1, x._2._2, mask.table.getFullName, x._2._1)
        } else {
          val badFile = new AuditFileData
          badFile.path = x._1
          badFile.totalLines = x._2._2
          badFile.okLines = x._2._1
          val errors = fileRdd.filter(y => y._2 != 0).take(5).toSeq
          badFile.errors = errors.map(x => "Offset: " + x._1(1) + "; Tipo de Error: " + LineStatus.apply(x._2).toString())
          badFiles = badFiles :+ badFile
          CoreContext.logger.debug("NNNOOOOOOOOOOOOOOOOOO HA PASADO EL TEST DE TOLERANCIA:" + x._1)
          CoreServices.governmentService.auditRawTable(x._1, x._2._2, mask.table.getFullName, 0)
        }
      } else {
        CoreServices.governmentService.auditRawTable(x._1, x._2._2, mask.table.getFullName, x._2._1)
      }
    })
    badFiles
  }

  def getDateFiles(goodFiles: Seq[String]): Seq[(String, String, String)] = {
    var result = Seq[(String, String, String)]()
    for (goodFile <- goodFiles) {
      val date = PrerawToRawConstants.getYearMonthDay(goodFile)
      if (!result.contains(date))
        result = result :+ PrerawToRawConstants.getYearMonthDay(goodFile)
    }
    result
  }

  def checkFaultTolerance(mask: Mask, rdd: RDD[Tuple2[Seq[Any], Int]], numberOks: Long, total: Long): Boolean = {

    FaultToleranceUtils.check(mask.faultToleranceTests, rdd, numberOks, total)

  }

}
