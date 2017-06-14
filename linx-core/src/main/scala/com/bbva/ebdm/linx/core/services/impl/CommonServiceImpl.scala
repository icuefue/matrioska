package com.bbva.ebdm.linx.core.services.impl

import java.util.Calendar

import org.apache.commons.lang.exception.ExceptionUtils
import com.bbva.ebdm.linx.core.beans.RdbmsOptions
import com.bbva.ebdm.linx.core.conf.CoreContext
import com.bbva.ebdm.linx.core.conf.CoreRepositories
import com.bbva.ebdm.linx.core.conf.CoreServices
import com.bbva.ebdm.linx.core.constants.{CoreConstants, CoreDfsConstants}
import com.bbva.ebdm.linx.core.exceptions.{ImportTableToRawNotSupportedException, RdbmsImportException}
import com.bbva.ebdm.linx.core.services.CommonService
import com.bbva.ebdm.linx.core.enums.PartitionTypes.{Fechaproceso, NoPartition, PartitionType, YearMonthDay}
import org.apache.spark.sql.DataFrame

class CommonServiceImpl extends CommonService {

  override def importTable(options: RdbmsOptions) = {

    if(!options.importToPreraw) {
      // importTableToRaw(options)
      throw new ImportTableToRawNotSupportedException(s"Error importando ${options.sourceSchema}.${options.sourceTable} -> " +
        s"${options.targetSchema}.${options.targetTable}, no se permiten importaciones directamente al RAW")
    }
    else {
      importTableToPreraw(options)
    }
  }

//  /**
//    * Importación del objeto especificado a una tabla del raw
//    *
//    * @param options Objeto donde se almacena la información nesesaria
//    * sobre la tabla de origen y la tabla destino para poder realizar la importación
//    */
//  private def importTableToRaw(options: RdbmsOptions) = {
//    // Obtencion del targetDir en HDFS
//    var targetDir = options.targetDir
//    targetDir = CoreRepositories.rdbmsRepository.buildPartitionTargetDir(options.partitionType, targetDir, options.planDate)
//
//    // Hacemos una copia de seguridad del contenido en en targetDir
//    val targetDirBak = targetDir + ".bak" + Calendar.getInstance.getTimeInMillis
//    CoreRepositories.dfsRepository.rename(targetDir, targetDirBak)
//
//    // Generamos un nuevo RdbmsOptions con nuestro nuevo targetDir
//    val optionsTemp = RdbmsOptions(options.source, options.sourceSchema, options.sourceTable,
//      options.targetSchema, options.targetTable, targetDir, options.partitionType, options.fieldsTerminatedBy,
//      options.linesTerminatedBy, options.planDate, options.fileFormat, options.compressionCodec, options.importToPreraw,
//      options.enclosedByQuotes, options.escapedByBackslash)
//
//    var success = false
//    var attempts = 1
//
//    // 3 intentos para importar la tabla
//    while (!success && attempts <= 3) {
//      try {
//        // Borrado de la partición (si existe una para sobreescribir)
//        dropPartion(options.partitionType, options.targetSchema,
//          options.targetTable, options.planDate)
//        CoreRepositories.rdbmsRepository.importTool(optionsTemp)
//
//        // Añadimos la partición a la tabla (si procede)
//        addPartition(options.partitionType, options.targetSchema,
//          options.targetTable, options.planDate)
//
//        // Auditamos lo ingestado con Sqoop
//        CoreServices.governmentService.auditRawTable(optionsTemp)
//
//        // Éxito en la importación de la tabla
//        success = true
//      } catch {
//        case t: Throwable =>
//          CoreContext.logger.error(ExceptionUtils.getStackTrace(t))
//          CoreRepositories.dfsRepository.delete(targetDir, recursive = true)
//          attempts = attempts + 1
//      }
//    }
//
//    if (success) {
//      CoreRepositories.dfsRepository.delete(targetDirBak, recursive = true)
//    } else {
//      CoreRepositories.dfsRepository.rename(targetDirBak, targetDir)
//      throw new RdbmsImportException(s"Error al importar la tabla ${options.sourceSchema}.${options.sourceTable}"
//        + "al directorio " + targetDir)
//    }
//  }

  private def importTableToPreraw(options: RdbmsOptions) = {
    var success = false
    var attempts = 1

    // 3 intentos para importar la tabla
    while (!success && attempts <= 3) {
      try {
        CoreRepositories.rdbmsRepository.importTool(options)

        // ¿Éxito en la importación de la tabla?
        if(CoreRepositories.dfsRepository.exists(s"${options.targetDir}/${CoreDfsConstants.SqoopSuccessFile}")) {
          success = true
          CoreRepositories.dfsRepository.delete(s"${options.targetDir}/${CoreDfsConstants.SqoopSuccessFile}")
        }
        else {
          CoreContext.logger.error(s"Error al importar la tabla ${options.sourceSchema}.${options.sourceTable} " +
            s"al directorio  ${options.targetDir}")
        }
      } catch {
        case t: Throwable =>
          CoreContext.logger.error(ExceptionUtils.getStackTrace(t))
          CoreRepositories.dfsRepository.delete(options.targetDir, recursive = true)
          attempts = attempts + 1
      }
    }

    if (!success) {
      throw new RdbmsImportException(s"Error al importar la tabla ${options.sourceSchema}.${options.sourceTable}" +
        s"al directorio  ${options.targetDir}")
    }
  }

  override def getMaxPartition(schema: String, table: String): Option[String] = {
    val result = CoreRepositories.hiveRepository.sql(CoreConstants.HqlCommonTablePartitionMaxDate, schema, table)

    result match {
      case Some(result_some) =>
        if (!result_some.rdd.isEmpty && result_some.first.getString(0) != null) {
          Some(result_some.first.getString(0))
        } else {
          None
        }
      case None => None
    }
  }

//  /**
//    * Borrado de la partición de una tabla especificando el tipo de partición
//    *
//    * @param partitionType - Tipo de partición
//    * @param schema - Esquema
//    * @param table - Tabla
//    * @param planDate - Fecha de planificación
//    */
//  private def dropPartion(partitionType: PartitionType, schema: String, table: String, planDate: String): Unit = {
//    partitionType match {
//      case NoPartition =>
//      case Fechaproceso => CoreRepositories.hiveRepository.sql(CoreConstants.HqlCommonTableDropPartition,
//        schema, table, s"fechaproceso=$planDate")
//      case YearMonthDay => CoreRepositories.hiveRepository.sql(CoreConstants.HqlCommonTableDropPartition,
//        schema, table, s"year=${planDate.substring(0, 4)},month=${planDate.substring(4, 6)},day=${planDate.substring(6, 8)}")
//    }
//  }
//
//  /**
//    * Añadir una partición a una tabla especificando el tipo de partición
//    *
//    * @param partitionType - Tipo de partición
//    * @param schema - Esquema
//    * @param table - Tabla
//    * @param planDate - Fecha de planificación
//    */
//  private def addPartition(partitionType: PartitionType, schema: String, table: String, planDate: String): Unit = {
//    partitionType match {
//      case NoPartition =>
//      case Fechaproceso => CoreRepositories.hiveRepository.sql(CoreConstants.HqlCommonTableAddPartition,
//        schema, table, s"fechaproceso=$planDate")
//      case YearMonthDay => CoreRepositories.hiveRepository.sql(CoreConstants.HqlCommonTableAddPartition,
//        schema, table, s"year=${planDate.substring(0, 4)},month=${planDate.substring(4, 6)},day=${planDate.substring(6, 8)}")
//    }
//  }

   /**
    * Comprueba si un DataFrame está vacío o no
    *
    *
    * @param df - DataFrame a validar
    *
    * @return Boolean - True si está vacío, false en caso contrario
    */
  override def isEmptyDF(df: DataFrame) : Boolean = {
    df.count == 0
  }

}
