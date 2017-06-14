package com.bbva.ebdm.linx.ingest.services.impl

import java.util.Calendar
import com.bbva.ebdm.linx.ingest.services.PrerawMaintenanceService
import com.bbva.ebdm.linx.core.conf.CoreContext
import com.bbva.ebdm.linx.core.conf.CoreRepositories
import com.bbva.ebdm.linx.ingest.constants.IngestDfsConstants

/**
 * Implementación del servicio de mantenimento sobre la capa Preraw
 *
 * Created by xe54068 on 10/11/2016.
 */
object PrerawMaintenaceServiceConstants {
  val ThirtyDaysMilli: Long = 2592000000L
}

class PrerawMaintenanceServiceImpl extends PrerawMaintenanceService {

  override def deleteFailuresOlderThirtyDays(): Unit = {
    CoreContext.logger.info("PrerawMaintenanceService -> deleteFailuresOlderThirtyDays")

    // thirtyDaysAgo = timestamp de hace 30 días
    val thirtyDaysAgo = Calendar.getInstance().getTimeInMillis - PrerawMaintenaceServiceConstants.ThirtyDaysMilli
    CoreContext.logger.info("thirtyDaysAgo -> ".concat(thirtyDaysAgo.toString))

    // listamos el status de todos los ficheros de la carpeta failures
    val files = CoreRepositories.dfsRepository.listStatus(IngestDfsConstants.PrerawFailures, recursive = true)

    // Borramos los que tengan más de 30 días
    files.foreach { file =>
      if (file.getModificationTime < thirtyDaysAgo) {
        CoreContext.logger.info("Borrar ".concat(file.getPath.toString))
        CoreRepositories.dfsRepository.deleteAbsolutePath(file.getPath.toString)
      } else {
        CoreContext.logger.info("NO borrar ".concat(file.getPath.toString))
      }
    }
  }

  override def deleteSuccessfulOlderThirtyDays(): Unit = {
    CoreContext.logger.info("PrerawMaintenanceService -> deleteSuccessfulOlderThirtyDays")

    // thirtyDaysAgo = timestamp de hace 30 días
    val thirtyDaysAgo = Calendar.getInstance().getTimeInMillis - PrerawMaintenaceServiceConstants.ThirtyDaysMilli
    CoreContext.logger.info("thirtyDaysAgo -> ".concat(thirtyDaysAgo.toString))

    // listamos el status de todos los ficheros de la carpeta successful
    val files = CoreRepositories.dfsRepository.listStatus(IngestDfsConstants.PrerawSuccessful, recursive = true)

    // Borramos los que tengan más de 30 días
    files.foreach { file =>
      if (file.getModificationTime < thirtyDaysAgo) {
        CoreContext.logger.info("Borrar ".concat(file.getPath.toString))
        CoreRepositories.dfsRepository.deleteAbsolutePath(file.getPath.toString)
      } else {
        CoreContext.logger.info("NO borrar ".concat(file.getPath.toString))
      }
    }
  }

  override def deleteSuccessfulFilesContent(): Unit = {
    CoreContext.logger.info("PrerawMaintenanceService -> deleteSuccessfulFilesContent")

    // listamos el status de todos los ficheros de la carpeta successful
    val files = CoreRepositories.dfsRepository.listStatus(IngestDfsConstants.PrerawSuccessful, recursive = true)

    // Borramos el contenido de los que tengan algo
    files.foreach { file =>
      if (file.getLen > 0) {
        CoreContext.logger.info("Borrar contenido ".concat(file.getPath.toString))
        CoreRepositories.dfsRepository.deleteAbsolutePath(file.getPath.toString)
        CoreRepositories.dfsRepository.createAbsolutePath(file.getPath.toString, "")
      }
    }
  }
}
