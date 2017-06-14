package com.bluetab.matrioska.ingest.services

/**
  * Servicio de mantenimiento de la capa Preraw
  *
  * Created by xe54068 on 10/11/2016.
  */
trait PrerawMaintenanceService {
  /**
    * Borrado de los ficheros que tengan más de 30 días de la carpeta "failures"
    */
  def deleteFailuresOlderThirtyDays(): Unit

  /**
    * Borrado de los ficheros que tengan más de 30 días de la carpeta "successful"
    */
  def deleteSuccessfulOlderThirtyDays(): Unit

  /**
    * Borrado del contenido de los ficheros de la carpeta "successful" (no borrarel fichero, sólo el contenido)
    */
  def deleteSuccessfulFilesContent(): Unit
}
