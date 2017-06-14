package com.bbva.ebdm.linx.core.repositories

import java.io.File
import java.nio.charset.Charset

import scala.util.matching.Regex

/**
 * Acciones sobre el FileSystem
 *
 * @author xe54068
 */
trait FSRepository {

  /**
   * Listar el contenido de un directorio
   *
   * @param f : File que representa un directorio del Sistema de ficheros
   * @return Listado del contenido del directorio
   */
  def listFiles(f: File): Array[File]

  /**
   * Lista el contenido de un directorio y en el caso de que
   * el parametro de entrada "recursive" sea true recorre de forma recursiva todos los subdirectorios
   *
   * @param f File que representa un directorio del Sistema de ficheros
   * @param recursive Boolean que indica si hay que recorrer los directorios de forma recursiva
   * @return
   */
  def listFiles(f: File, recursive: Boolean): Array[File]

  /**
   * Lista el contenido de un directorio. En el caso de que
   * el parametro de entrada "recursive" sea true recorre de forma recursiva todos los subdirectorios
   * Si el parámetro exclude directories es true filtra los directorios del resultado
   *
   * @param f File que representa un directorio del Sistema de ficheros
   * @param recursive Boolean que indica si hay que recorrer los directorios de forma recursiva
   * @param excludeDirectories Boolean para indicar si se muestran los directorios en el resultado
   * @return Listado de los ficheros y directorios que cumplen las condiciones
   */
  def listFiles(f: File, recursive: Boolean, excludeDirectories: Boolean): Array[File]

  /**
   * Lista el contenido de un directorio. En el caso de que
   * el parametro de entrada "recursive" sea true recorre de forma recursiva todos los subdirectorios
   * Filtra los resultados por la expresión regular especificada
   * @param f File que representa un directorio del Sistema de ficheros
   * @param r expresion regular para filtrar los ficheros de salida
   * @param recursive Boolean que indica si hay que recorrer los directorios de forma recursiva
   * @param excludeDirectories Boolean para indicar si se muestran los directorios en el resultado
   * @return Listado de los ficheros y directorios que cumplen las condiciones
   */
  def listFiles(f: File, r: Regex, recursive: Boolean, excludeDirectories: Boolean): Array[File]

  /**
   * Renombra el fichero por el fichero+parametro entrante
   * @param f File que representa un fichero del sistema
   * @param sufix String que será el sufijo añadido al nombre del fichero
   * @return success Boolean que indicará si el cambio de nombre ha sido correcto
   */
  def renameFile(f: File, sufix: String): File

  /**
   * Renombra el fichero eliminando sufijo (fichero_SUFIJO ==> fichero)
   * @param f File que representa un fichero del sistema
   * @param sufix String que será el sufijo eliminado al nombre del fichero
   * @return File que indicará el nombe del fichero
   */
  def deleteSufix(f: File, sufix: String): File

  /**
   * Creación de un fichero con un contenido
   *
   * @param filePath - nombre y path del fichero
   * @param content - contenido del fichero
   */
  def createFileWithContent(filePath: String, content: Array[Char]): Unit

  /**
   * Detecta el charset que tiene un fichero
   *
   * @param filepath - path al fichero
   * @return - Charset detectado para ese fichero
   */
  def detectFileCharset(filepath: String): Charset

  /**
    * Convierte un fichero con el charset pasado como parámetro por otro con el charset definido
    *
    * @param originalFile - Path al fichero de origen
    * @param newFile - Path del fichero de destino
    * @param originalCharset - charset del fichero de origen
    * @param newCharset - charset del fichero de salida
    */
  def convertFile(originalFile: String, newFile: String, originalCharset: Charset, newCharset: Charset)

  /**
    * Borra el fichero cuyo path es el pasado por parámetro
    *
    * @param filepath - path del fichero que hay que borrar
    * @return - Devuelve true si se borra el fichero correctamente, false en el caso contrario
    */
  def deleteFile(filepath: String): Boolean
}
