package com.bluetab.matrioska.ingest.services.impl

import javax.xml.bind.DatatypeConverter

import com.bluetab.matrioska.core.conf.CoreContext
import com.bluetab.matrioska.core.conf.CoreServices
import com.bluetab.matrioska.core.LinxAppArgs
import com.bluetab.matrioska.core.beans.RdbmsOptions
import com.bluetab.matrioska.core.conf.{CoreRepositories, CoreServices}
import com.bluetab.matrioska.core.constants.CoreConstants
import com.bluetab.matrioska.core.exceptions.FatalException
import com.bluetab.matrioska.ingest.beans.ImportItem
import com.bluetab.matrioska.ingest.conf.IngestServices
import com.bluetab.matrioska.ingest.constants.IngestDfsConstants
import com.bluetab.matrioska.ingest.services.CommonService
import org.apache.commons.lang.exception.ExceptionUtils

class CommonServiceImpl extends CommonService {

  override def deleteRawFlag(schema: String, table: String) = {
    val flagName = schema + "-" + table + ".flg"
    CoreRepositories.dfsRepository.delete(IngestDfsConstants.FlagsRaw + s"/$flagName")
  }

  override def createRawFlag(schema: String, table: String) = {
    val flagName = schema + "-" + table + ".flg"
    CoreRepositories.dfsRepository.create(IngestDfsConstants.FlagsRaw + s"/$flagName", "")

  }

  override def importTables(useCase: String) = {

    // Obtenemos los importItems a importar
    val importItems = IngestServices.metadataService.getImportItemsByUseCase(useCase)
    CoreContext.logger.info(s"Número de tablas a importar: ${importItems.size}")
    CoreContext.logger.info(s"Tablas a importar: ${importItems.toString}")

    // variable de errores encontrados
    var errors = false

    // Recorremos los importItems
    for (importItem <- importItems) {
      try {
        if(!importItem.importToPreraw) {
          // Importar el item al Raw
          importItemToRaw(importItem)
        }
        else {
          // Importar el item al Preraw
          importItemToPreraw(importItem)
        }

      } catch {
        case t: Throwable =>
          CoreContext.logger.error(ExceptionUtils.getStackTrace(t))
          errors = true
      }
    }
    if (errors) {
      throw new FatalException("Ha fallado la importación de alguna de las tablas")
    }
  }

  /**
    * Importación de una tabla directamente al raw
    *
    * @param importItem - Objeto ImportItem
    */
  private def importItemToRaw(importItem: ImportItem): Unit = {
    CoreContext.logger.info(s"Importando tabla: ${importItem.sourceSchema}.${importItem.sourceTable} => " +
      s"${importItem.targetSchema}.${importItem.targetTable}")

    // Obtenemos el char del fieldDelimitier y el lineDelimitier
    val fieldDelimiter = new String(DatatypeConverter.parseHexBinary(importItem.fieldDelimiterHex.split("x")(1))).charAt(0)
    val lineDelimiter = new String(DatatypeConverter.parseHexBinary(importItem.lineDelimiterHex.split("x")(1))).charAt(0)

    // Definimos el RdbmOptions para pasarle al servicio de importTables
    val options = RdbmsOptions(importItem.source,
      importItem.sourceSchema, importItem.sourceTable, importItem.targetSchema, importItem.targetTable,
      importItem.targetDir, importItem.partitionType, fieldDelimiter, lineDelimiter,
      LinxAppArgs.planDate, importItem.fileFormat, importItem.compressionCodec, importItem.importToPreraw, importItem.enclosedByQuotes,
      importItem.escapedByBackslash)

    // Llamada al core para hacer la importación de las tablas
    CoreServices.commonService.importTable(options)

    // ¿Se ha de generar un flag?
    if (importItem.generateFlag) {
      createRawFlag(importItem.targetSchema, importItem.targetTable)
    }
    CoreContext.logger.info(s"Importación ${importItem.sourceSchema}.${importItem.sourceTable} a " +
      s"${importItem.targetSchema}.${importItem.targetTable} completada")
  }

  /**
    * Importación de una tabla al preraw
    *
    * @param importItem - Objeto ImportItem
    */
  def importItemToPreraw(importItem: ImportItem): Unit = {

    val destinationPath = StagingToPrerawConstants.preRawPathToLoad +
      CoreConstants.slash + "informacional" +
      CoreConstants.slash + StagingToPrerawConstants.Year_Field + LinxAppArgs.planDate.substring(0,4).toInt +
      CoreConstants.slash + StagingToPrerawConstants.Month_Field + LinxAppArgs.planDate.substring(4,6).toInt +
      CoreConstants.slash + StagingToPrerawConstants.Day_Field + LinxAppArgs.planDate.substring(6,8).toInt +
      CoreConstants.slash + importItem.sourceSchema + "_" + importItem.sourceTable

    CoreContext.logger.info(s"Importando tabla: ${importItem.sourceSchema}.${importItem.sourceTable} => $destinationPath")

    // Obtenemos el char del fieldDelimitier y el lineDelimitier
    val fieldDelimiter = new String(DatatypeConverter.parseHexBinary(importItem.fieldDelimiterHex.split("x")(1))).charAt(0)
    val lineDelimiter = new String(DatatypeConverter.parseHexBinary(importItem.lineDelimiterHex.split("x")(1))).charAt(0)

    // Definimos el RdbmOptions para pasarle al servicio de importTables
    val options = RdbmsOptions(importItem.source,
      importItem.sourceSchema, importItem.sourceTable, importItem.targetSchema, importItem.targetTable,
      destinationPath, importItem.partitionType, fieldDelimiter, lineDelimiter,
      LinxAppArgs.planDate, importItem.fileFormat, importItem.compressionCodec, importItem.importToPreraw, importItem.enclosedByQuotes,
      importItem.escapedByBackslash)

    // Llamada al core para hacer la importación de las tablas
    CoreServices.commonService.importTable(options)

    CoreContext.logger.info(s"Importación ${importItem.sourceSchema}.${importItem.sourceTable} a " +
      s"$destinationPath completada")
  }
}
