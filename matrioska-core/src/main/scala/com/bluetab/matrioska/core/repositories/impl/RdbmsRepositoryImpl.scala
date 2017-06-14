package com.bluetab.matrioska.core.repositories.impl

import com.bluetab.matrioska.core.beans.RdbmsOptions
import com.bluetab.matrioska.core.conf.{CoreConfig, CoreContext, CoreRepositories}
import com.bluetab.matrioska.core.constants.CoreConstants
import com.bluetab.matrioska.core.enums.FileFormats.{Avro, Parquet, Sequence, Text}
import com.bluetab.matrioska.core.exceptions.{NotSupportedSqoopFileFormat, RdbmsImportException}
import com.bluetab.matrioska.core.repositories.RdbmsRepository
import org.apache.sqoop.tool.ImportTool

class RdbmsRepositoryImpl extends RdbmsRepository {

  override def importTool(options: RdbmsOptions) = {

    // Validamos las opciones primero
    this.validateImportToolSqoopOption(options)

    val source = CoreConfig.rdbms.get(options.source)
    val uri = source.get("uri")
    val username = source.get("username")
    val password = source.get("password")

    val sqoopOptions = CoreContext.sqoopOptions

    sqoopOptions.setConnectString(uri)
    sqoopOptions.setUsername(username)
    sqoopOptions.setPassword(password)
    sqoopOptions.setTableName(options.sourceSchema + "." + options.sourceTable)
    sqoopOptions.setFieldsTerminatedBy(options.fieldsTerminatedBy)
    sqoopOptions.setLinesTerminatedBy(options.linesTerminatedBy)

    if (options.enclosedByQuotes) {
      sqoopOptions.setEnclosedBy(CoreConstants.doubleQuotes)
      sqoopOptions.setOutputEncloseRequired(true)
    }

    if (options.escapedByBackslash) {
      sqoopOptions.setEscapedBy(CoreConstants.backslash)
    }

    sqoopOptions.setHiveDropDelims(true)
    sqoopOptions.setDirectMode(true)
    sqoopOptions.setTargetDir(CoreRepositories.dfsRepository.getFilePath(options.targetDir))
    sqoopOptions.setNumMappers(1)
    options.compressionCodec match {
      case Some(compressionCodec) =>
        sqoopOptions.setCompressionCodec(compressionCodec.codecLib)
        sqoopOptions.setUseCompression(true)
      case None =>
    }

    options.fileFormat match {
      case Some(fileFormat) => sqoopOptions.setFileLayout(fileFormat.fileLayout)
      case None =>
    }
    
    CoreContext.logger.info(s"sqoopOptions: ${options.toString}")
    val ret = new ImportTool().run(sqoopOptions)

    if (ret != 0) {
      throw new RdbmsImportException(s"Error al importar la tabla ${options.sourceSchema}.${options.sourceTable} al directorio "
        + options.targetDir)
    }

  }

  private def validateImportToolSqoopOption(optionsToValidate: RdbmsOptions):Unit = {
    optionsToValidate.fileFormat match {
        // Sólo se soporta el fileFormat "Text"
      case Some(fileFormat) =>
        fileFormat match {
          case Text => CoreContext.logger.info(s"Formato de importación -> ${Text.toString}")
          case Parquet => throw new NotSupportedSqoopFileFormat(s"${Parquet.toString} -> tipo no soportado por Sqoop")
          case Avro => throw new NotSupportedSqoopFileFormat(s"${Avro.toString} -> tipo no soportado por Sqoop")
          case Sequence => throw new NotSupportedSqoopFileFormat(s"${Sequence.toString} -> tipo no soportado por Sqoop")
        }
      case None =>
    }
  }
  
//   /**
//    * Construcción del targetDir de partición a partir del tipo de partición pasado
//    *
//    * @param partitionType - tipo de partición
//    * @param baseDir       - dirección base (directorio donde reside la tabla)
//    * @param planDate      - Fecha de planificación
//    * @return String con el targetDir de esa partición
//    */
//  def buildPartitionTargetDir(partitionType: PartitionType, baseDir: String, planDate: String): String = {
//    partitionType match {
//      case NoPartition => baseDir
//      case Fechaproceso => s"$baseDir/fechaproceso=$planDate"
//      case YearMonthDay =>
//        s"$baseDir/year=${planDate.substring(0, 4)}/month=${planDate.substring(4, 6)}/day=${planDate.substring(6, 8)}"
//
//    }
//  }
}
