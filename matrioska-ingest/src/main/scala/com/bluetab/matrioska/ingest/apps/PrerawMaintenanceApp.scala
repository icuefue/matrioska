package com.bluetab.matrioska.ingest.apps

import com.bluetab.matrioska.core.LinxApp
import com.bluetab.matrioska.ingest.conf.IngestServices

/**
  * Mantenimiento de las carpetas de la capa preraw
  *
  * Acciones a realizar:
  * 1) Borrar los ficheros que tengan más de 30 días de la carpeta "failures"
  * 2) Borrar contenido de los ficheros de la carpeta "successful" (no borrar el fichero, sólo el contenido)
  * 3) Borrar los ficheros que tengan más de 30 días de la carpeta "successful"
  *
  * Created by xe54068 on 10/11/2016.
  */
class PrerawMaintenanceApp extends LinxApp {
  override def run(args: Seq[String]): Unit = {
    // Borrar los ficheros que tengan más de 30 días de la carpeta "failures"
    IngestServices.prerawMaintenanceService.deleteFailuresOlderThirtyDays()

    // Borrar los ficheros que tengan más de 30 días de la carpeta "successful"
    IngestServices.prerawMaintenanceService.deleteSuccessfulOlderThirtyDays()

    // Borrar contenido de los ficheros de la carpeta "successful" (no borrar el fichero, sólo el contenido)
    IngestServices.prerawMaintenanceService.deleteSuccessfulFilesContent()
  }
}
