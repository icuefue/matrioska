package com.bluetab.matrioska.ingest.apps.aperiodics

import com.bluetab.matrioska.core.conf.CoreContext
import java.io.File

import org.joda.time.DateTime
import org.apache.commons.lang.exception.ExceptionUtils
import com.bluetab.matrioska.core.conf.CoreRepositories
import com.bluetab.matrioska.core.LinxApp
import com.bluetab.matrioska.core.conf.{CoreConfig, CoreRepositories}
import com.bluetab.matrioska.core.exceptions.FatalException
import com.bluetab.matrioska.ingest.conf.IngestServices

class CargaHistoricaNetcashApp extends LinxApp {

  override def run(args: Seq[String]) {
    val patternPath = "/sta_netcash_analytics/historico"
    val filePath = new File(CoreConfig.localfilesystempath.staging + "/mat" + patternPath)
    val filenames = CoreRepositories.fsRepository.listFiles(filePath)

    var hasErrors = false

    filenames.foreach { file =>
      try {
        val fileString = file.toString
        val index = fileString.lastIndexOf('.')
        val day = fileString.substring(index - 2, index)
        val month = fileString.substring(index - 4, index - 2)
        val year = fileString.substring(index - 8, index - 4)

        val filename = fileString.substring(fileString.lastIndexOf('/'))

        val path = s"/raw/preraw/to_load/mat/year=$year/month=$month/day=$day" + patternPath + filename

        CoreContext.logger.info("Copiando fichero " + fileString + " a preraw: " + path)

        CoreRepositories.dfsRepository.copyFromLocalToHDFS(fileString, path)

        CoreContext.logger.info("Fichero " + fileString + " copiado a preraw: " + path)

      } catch {
        case e: Exception =>
          hasErrors = true
          CoreContext.logger.error(ExceptionUtils.getStackTrace(e))
      }
    }
    if (hasErrors) throw new FatalException("Copia fallida de ficheros")

  }
}