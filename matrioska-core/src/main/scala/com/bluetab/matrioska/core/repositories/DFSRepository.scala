package com.bluetab.matrioska.core.repositories

import org.apache.hadoop.fs.FileStatus
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe.Type


/**
 * Acciones sobre un Distributed FileSystem
 *
 * @author xe54068
 */
trait DFSRepository {

  /**
   * Comprueba la existencia de una carpeta o fichero dado
   *
   * @param path - path a la carpeta o fichero para comprobar su existencia
   * @return - booleano de si existe o no la carpeta o fichero
   */
  def exists(path: String): Boolean

  /**
   * Borra un fichero o una carpeta vacia
   *
   * @param path - fichero o carpeta vacia a borrar
   */
  def delete(path: String)

  /**
   * Borra un fichero o una carpeta y el contenido que haya dentro (recursive -> true)
   *
   * @param path - path de la carpeta a borrar
   * @param recursive - borrado recursivo
   */
  def delete(path: String, recursive: Boolean)

  /**
    * Borra todo el contenido de una carpeta
    *
    * @param path - path de la carpeta a borrar su contenido
    */
  def deleteContent(path: String)

  /**
    * Borra un fichero o una carpeta vacia
    *
    * @param absolutePath - fichero o carpeta vacia a borrar (path absoluto)
    *
    * @note este método borra paths absolutos sin hacer distinción entre desarrollo y producción
    */
  def deleteAbsolutePath(absolutePath: String)

  /**
    * Borra un fichero o una carpeta y el contenido que haya dentro (recursive -> true)
    *
    * @param absolutePath - path de la carpeta a borrar (path absoluto)
    * @param recursive - borrado recursivo
    *
    * @note este método borra paths absolutos sin hacer distinción entre desarrollo y producción
    */
  def deleteAbsolutePath(absolutePath: String, recursive: Boolean)
  
  /**
   * Crea un fichero
   *
   * @param path - path y nombre del fichero
   * @param content - contenido del fichero
   */
  def create(path: String, content: Array[Byte])

  /**
    * Añade datos a un fichero
    *
    * @param path - path al fichero
    * @param content - contenido a añadir
    */
  def append(path: String, content: Array[Byte])

  /**
    * Crea un fichero
    *
    * @param path - path y nombre del fichero
    * @param content - contenido del fichero
    */
  def create(path: String, content: String)

  /**
    * Crea un fichero
    *
    * @param absolutePath - path y nombre del fichero (path absoluto)
    * @param content - contenido del fichero
    */
  def createAbsolutePath(absolutePath: String, content: Array[Byte])

  /**
    * Crea un fichero
    *
    * @param absolutePath - path y nombre del fichero (path absoluto)
    * @param content - contenido del fichero
    */
  def createAbsolutePath(absolutePath: String, content: String)

  /**
   * Cortar y pegar un fichero
   *
   * @param source - fichero fuente
   * @param destination - fichero destino
   * @return - booleano indicando el éxito o fracaso de la acción
   */
  def rename(source: String, destination: String): Boolean
  
    /**
   * Subir un fichero a HDFS
   *
   * @param source - fichero fuente
   * @param destination - fichero destino
   */
  def copyFromLocalToHDFS(source: String, destination: String)

  /**
   * Creación de un directorio
   *
   * @param path - path al fichero a crear
   * @return - booleano indicando el éxito o fracaso de la acción
   */
  def mkdirs(path: String): Boolean

  /**
   * Listar el contenido de un directorio
   *
   * @param source - path al directorio
   * @param recursive - si ha de listar de manera recursiva
   * @return - Secuencia con el contenido
   */
  def listFiles(source: String, recursive: Boolean): Seq[String]

  /**
    * Listar las características de los ficheros de un determinado directorio
    *
    * @param path - path al directorio
    * @param recursive - si el listado será recursivo
    * @return - Array con el listado de ficheros encontrado
    */
  def listStatus (path: String, recursive:Boolean): ListBuffer[FileStatus]

  /**
    * Carga de un fichero en un RDD
    *
    * @param path - path al fichero
    * @return - RDD con el contenido del fichero
    */
  def textFile(path: String): RDD[String]
  
    /**
    * Carga de un fichero completo en un RDD
    *
    * @param path - path al fichero
    * @return - RDD con el contenido de (nombre del fichero, contenido del fichero)
    */
  
  def wholeTextFile(path: String): RDD[(String, String)]
  
    /**
    * Carga de un fichero completo en un RDD
    *
    * @param path - path al fichero
    * @param minPartition - numero minimo de particiones (en este caso será 1)
    * @return - RDD con el contenido de (nombre del fichero, contenido del fichero)
    */
  
  def wholeTextFile(path: String, minPartitions: Int): RDD[(String, String)]

  /**
    *
    * Lectura de un CSV (o fichero con un delimitador regular) para cargarlo en un dataframe
    *
    * @param path - path al fichero
    * @param c - case class de la que se cogerá la estructura
    * @return - dataFrame con el contenido del CSV
    */
  def readCsvAsDF(path: String, c: Type): DataFrame

  /**
    *
    * Lectura de un CSV (o fichero con un delimitador regular) para cargarlo en un dataframe
    *
    * @param path - path al fichero
    * @param c - estructura del fichero (tipo de cada columna)
    * @return - dataFrame con el contenido del CSV
    */
  def readCsvAsDF(path: String, c: StructType): DataFrame

  /**
    *
    * Lectura de un CSV (o fichero con un delimitador regular) para cargarlo en un dataframe
    *
    * @param path - path al fichero
    * @param c - estructura del fichero (tipo de cada columna)
    * @param delimiter - delimitador de los campos
    * @return - dataFrame con el contenido del CSV
    */
  def readCsvAsDF(path: String, c: StructType, delimiter: String): DataFrame

  /**
    *
    * Lectura de un CSV (o fichero con un delimitador regular) para cargarlo en un dataframe
    *
    * @param path - path al fichero
    * @param c - estructura del fichero (tipo de cada columna)
    * @param delimiter - delimitador de los campos
    * @param header - ¿fichero con cabecera?
    * @return - dataFrame con el contenido del CSV
    */
  def readCsvAsDF(path: String, c: StructType, delimiter: String, header: Boolean): DataFrame

  /**
    *
    * Lectura de un CSV (o fichero con un delimitador regular) para cargarlo en un dataframe
    *
    * @param path - path al fichero
    * @param c - estructura del fichero (tipo de cada columna)
    * @param delimiter - delimitador de los campos
    * @param header - ¿fichero con cabecera?
    * @param dateFormat - formato de fecha para los campos date
    * @return - dataFrame con el contenido del CSV
    */
  def readCsvAsDF(path: String, c: StructType, delimiter: String, header: Boolean, dateFormat: String): DataFrame

  /**
    * Salvar un DStream en un fichero
    *
    * @param source - DStream a salvar
    * @param path - path donde salvar
    */
  def saveAsTextFiles(source: DStream[String], path: String)

  /**
    * *  Salvar un RDD en un fichero
    *
    * @param rdd - RDD a salvar
    * @param path - path donde salvar
    */
  def saveAsTextFile(rdd: RDD[String], path: String)
  
    /**
    * Comprueba si un fichero esta vacio
    *
    * @param path - path del fichero a validar
    */
  def isEmptyFile(path: String): Boolean
  
  /**
   * Obtiene el path absoluto de la ruta añadiendo el servidor y el prefijo de desarrollo o producción
 * @param path relativo de producción
 * @return Devuelve el path añadiendo el servidor y el prefijo de desarrollo o producción
 */
def getFilePath(path: String): String


}
