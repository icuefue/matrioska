package com.bluetab.matrioska.ingest.apps

import java.io.File

import com.bluetab.matrioska.core.LinxApp
import com.bluetab.matrioska.core.conf.{CoreConfig, CoreContext}
import com.bluetab.matrioska.core.exceptions.FatalException
import com.bluetab.matrioska.ingest.beans.Mask
import com.bluetab.matrioska.ingest.conf.IngestServices
import org.apache.commons.lang.exception.ExceptionUtils
import org.joda.time.DateTime

/**
 * Movimento de los ficheros encontrados en el staging de entrada a la capa preraw dentro de HDFS. Dentro de la capa
 * preraw se clasifican por origen de datos y fecha
 */
class StagingToPrerawApp extends LinxApp {

  override def run(args: Seq[String]) {

    // Cargamos las rutas de Staging
    val sources = IngestServices.metadataService.loadSources

    val files = IngestServices.stagingToPrerawService.getToLoadFilenames
    var hasErrors = false

    var fileSufix = new File("")
    files.foreach { file =>
      try {
        // Comprobamos que el fichero cumpla las condiciones para ser movido
        val fileString = file.getPath
        if (IngestServices.stagingToPrerawService.enableToStart(fileString)) {
          CoreContext.logger.info("--------------------------------------")
          CoreContext.logger.info("Procesando file " + file)

          val relativePath = fileString.substring(CoreConfig.localfilesystempath.staging.length())

          val source = IngestServices.metadataService.getSourceByFilename(sources, relativePath)
          val mask: Option[Mask] =
            source match {
              case Some(source) =>
                IngestServices.metadataService.getMaskByFilename(source, relativePath.split("/").drop(2).mkString("/"))
              case None =>
                CoreContext.logger.debug("Source no encontrado " + file)
                None
            }
          mask match {
            case Some(mask) =>

              // Obtemos la fecha de creacion del fichero
              val dateCreationFile = new DateTime(file.lastModified)

              // Preparamos el fichero para su copia a HDFS
              fileSufix = IngestServices.stagingToPrerawService.prepareFile(file)

              // Copiamos el fichero a HDFS
              IngestServices.stagingToPrerawService.copyFileToHDFS(fileSufix, dateCreationFile)

              // Marcamos el fichero como copiado correctamente
              // Ya no es necesario puesto que el fichero se borra
              //IngestServices.stagingToPrerawService.movedSuccessfully(fileSufix)
              CoreContext.logger.info("File copiado a HDFS correctamente")

            case None =>
              CoreContext.logger.debug("Patrón de fichero no encontrado: Source:" + file)
          }
        }
      } catch {
        case e: Exception =>
          hasErrors = true
          IngestServices.stagingToPrerawService.deleteFileSufix(fileSufix)
          CoreContext.logger.error(ExceptionUtils.getStackTrace(e))
      }

    }

    if (hasErrors) throw new FatalException("Copia fallida de ficheros")
  }
}
