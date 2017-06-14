package com.bluetab.matrioska.ingest.services

import java.io.File
import org.joda.time.DateTime

/**
  * Servicio de paso de ficheros de staging a preraw
  */
trait StagingToPrerawService {

  /**
    * Listar los ficheros presentes en el staging de entrada
    *
    * @param path - path a consultar dentro de staging
    * @return - lista de ficheros encontrados
    */
  def getToLoadFilenames : Seq[File]

  /**
    * Consultar si el fichero se puede procesar (no está ni "en proceso" ni "procesado")
    *
    * @param file - fichero a consultar
    * @return - si se puede procesar o no
    */
  def enableToStart(file: String): Boolean

  /**
    * Prepara el fichero para procesarlo poniendo "en proceso" como sufijo
    *
    * @param file - fichero a poner "en proceso"
    * @return - fichero con el sufijo puesto
    */
  def prepareFile(file: File): File

  /**
    * Copia el fichero especificado al preraw de hdfs en la carpeta {origen}/year=YYYYY/month=MM/day=DD
    *
    * @param pathFile - fichero a copiar a HDFS
    * @param dateCreation - fecha de creación del fichero origen (para sacar el year, month y day)
    */
  def copyFileToHDFS(pathFile: File, dateCreation: DateTime)

  /**
    * Cambia el sufijo del fichero de "en proceso" a "procesado"
    *
    * @param file - fichero a cambiar el sufijo
    */
  def movedSuccessfully(file: File)

  /**
    * Quita el sufijo "en progreso" del fichero especificado. Usado en caso de encontrar algún error en el procesamiento del fichero
    *
    * @param file - fichero a quitar el sufijo
    */
  def deleteFileSufix (file: File)
}