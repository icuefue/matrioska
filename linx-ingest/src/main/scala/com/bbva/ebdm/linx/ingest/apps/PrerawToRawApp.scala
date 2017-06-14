package com.bbva.ebdm.linx.ingest.apps

import com.bbva.ebdm.linx.core.LinxApp
import com.bbva.ebdm.linx.core.exceptions.FatalException
import com.bbva.ebdm.linx.ingest.conf.IngestServices
import com.bbva.ebdm.linx.core.conf.CoreContext
import org.apache.commons.lang.exception.ExceptionUtils
import com.bbva.ebdm.linx.core.conf.CoreServices
import com.bbva.ebdm.linx.core.LinxApp
import com.bbva.ebdm.linx.core.conf.CoreServices
import com.bbva.ebdm.linx.core.exceptions.FatalException
import com.bbva.ebdm.linx.core.conf.CoreContext
import java.util.Calendar
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.DataFrame
import org.apache.spark.SparkContext

class PrerawToRawApp extends LinxApp {

  override def run(args: Seq[String]) {

    val NumberOfPartitions = 8
    //Obtenemos el listado de ficheros que queremos cargar
    val prerawFilenames = IngestServices.prerawToRawService.getToLoadFilenames

    if (prerawFilenames.length > 0) {

      //Cargamos los metadatos para la ingesta
      val sources = IngestServices.metadataService.loadSources

      //Movemos fichero a la carpeta /preraw/in_progress
      IngestServices.prerawToRawService.markFilesAsInProgress(prerawFilenames)

      var hasErrors = false
      val errorMessageBuilder = StringBuilder.newBuilder

      for (sourceMap <- sources) {
        val source = sourceMap._2
        for (mask <- source.masks) {

          val filenames = IngestServices.metadataService.filterFilesByMask(prerawFilenames, mask)
          val badFiles = Seq[String]()
          if (filenames.length > 0) {
            try {

              val tableStruct = CoreServices.governmentService.getTableDefinition(mask.table.schema, mask.table.table)
              var seqRdd: Seq[RDD[Tuple2[Seq[Any], Int]]] = Seq[RDD[Tuple2[Seq[Any], Int]]]()
              for (filename <- filenames) {
                seqRdd = seqRdd :+ IngestServices.prerawToRawService.loadFile(filename, mask, tableStruct)
              }
              val rdd = CoreContext.sc.union(seqRdd).coalesce(NumberOfPartitions)
              // Checkeamos la calidad de los datos
              if (source.checkQA || mask.checkQA) {
                //IngestServices.prerawToRawService.checkQuality(rdd, mask.table.getFullName)
              }

              //Salvamos el fichero en la tabla asociada la a máscara
              val (goodFiles, badFiles) = IngestServices.prerawToRawService.saveToTable(rdd, mask, tableStruct, filenames)
              if (badFiles.size > 0) {
                //Movemos fichero a la carpeta / preraw / successful
                IngestServices.prerawToRawService.markAuditFilesAsFailure(badFiles)
                addErrorMessage(errorMessageBuilder, mask, badFiles)
                hasErrors = true
              }

              //Movemos fichero a la carpeta /preraw/successful
              IngestServices.prerawToRawService.markFilesAsSuccessful(goodFiles)
              //Creamos Fichero Flag de Raw
              IngestServices.commonService.createRawFlag(mask.table.schema, mask.table.table)

            } catch {
              case e: Throwable =>
                //Movemos fichero a la carpeta /preraw/failures
                IngestServices.prerawToRawService.markFilesAsFailure(filenames)
                hasErrors = true
                addErrorMessageNoControlado(errorMessageBuilder, mask, filenames)
                CoreContext.logger.error(ExceptionUtils.getStackTrace(e))
            }
          }
        }
      }

      val filesWithoutPattern = IngestServices.prerawToRawService.checkInProgress
      if (filesWithoutPattern.length > 0) {
        IngestServices.prerawToRawService.markFilesAsFailure(filesWithoutPattern)
        hasErrors = true
        errorMessageBuilder.append("\n\nFicheros excluidos por falta de patrón\n\n")
        filesWithoutPattern.foreach { x => errorMessageBuilder.append("- " + x + "\n") }
      }

      //Borramos los directorios vacíos
      IngestServices.prerawToRawService.deleteDirectories(prerawFilenames)
      if (hasErrors) {
        IngestServices.prerawToRawService.sendErrorNofitication(errorMessageBuilder.toString())
        throw new FatalException("Lectura fallida de alguno de los ficheros")
      }
    }
  }

  def addErrorMessage(errorMessageBuilder: StringBuilder, mask: com.bbva.ebdm.linx.ingest.beans.Mask, badFiles: Seq[com.bbva.ebdm.linx.ingest.beans.AuditFileData]) = {
    errorMessageBuilder.append("\n\nFicheros erróneos para tabla: " + mask.table.getFullName + "\n\n")
    badFiles.foreach { x =>
      errorMessageBuilder.append("- " + x.path + "\n")
      errorMessageBuilder.append("\t- Líneas correctas: " + x.okLines + "; Líneas totales: " + x.totalLines + "\n")
      errorMessageBuilder.append("\t- 5 primeros errores:\n")
      x.errors.foreach(y => errorMessageBuilder.append("\t\t - " + y + "\n"))
    }
  }
  def addErrorMessageNoControlado(errorMessageBuilder: StringBuilder, mask: com.bbva.ebdm.linx.ingest.beans.Mask, filenames: Seq[String]) = {
    errorMessageBuilder.append("\n\nError no controlado para ficheros de la tabla: " + mask.table.getFullName + "\n\n")
    filenames.foreach { x => errorMessageBuilder.append("- " + x + "\n") }
  }
}
