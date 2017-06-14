package com.bluetab.matrioska.core.repositories.impl

import com.bluetab.matrioska.core.conf.{CoreConfig, CoreContext}
import com.bluetab.matrioska.core.exceptions.PathIsNotEmptyDirectoryException
import com.bluetab.matrioska.core.repositories.DFSRepository
import org.apache.hadoop.fs.{FileStatus, Path}
import org.apache.hadoop.ipc.RemoteException
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe.Type

class DFSRepositoryImpl extends DFSRepository {

  def delete(path: String) = {
    delete(path, recursive = false)

  }

  def delete(path: String, recursive: Boolean) = {
    val absolutePath = getFilePath(path)
    deleteAbsolutePath(absolutePath, recursive)
  }

  override def deleteContent(path: String): Unit = {
    if (exists(path)) {
      CoreContext.logger.debug(s"DFSRepository -> deleteContent: $path")
      listStatusFileOrDirectory(path).foreach { element =>
        if (element.isFile || element.isSymlink) {
          deleteAbsolutePath(element.getPath.toString)
        }
        else {
          deleteAbsolutePath(element.getPath.toString, recursive = true)
        }
      }
    }
    else {
      CoreContext.logger.debug(s"DFSRepository -> deleteContent: $path no existe el path")
    }
  }

  def deleteAbsolutePath(absolutePath: String) = {
    deleteAbsolutePath(absolutePath, recursive = false)
  }

  def deleteAbsolutePath(absolutePath: String, recursive: Boolean) = {
    CoreContext.logger.debug("DFSRepository -> delete : " + absolutePath)
    try {
      CoreContext.dfs.delete(new Path(absolutePath), recursive)
    } catch {
      case e: RemoteException =>
        throw new PathIsNotEmptyDirectoryException(e)
      case t: Throwable => throw t
    }
  }

  def append(path: String, content: Array[Byte]) {
    CoreContext.dfs.append(new Path(getFilePath(path))).write(content)
  }

  def exists(path: String): Boolean = {
    CoreContext.dfs.exists(new Path(getFilePath(path)))
  }

  def create(path: String, content: Array[Byte]) {
    CoreContext.dfs.create(new Path(getFilePath(path))).write(content)
  }

  def create(path: String, content: String) {
    create(path, content.getBytes)
  }

  def createAbsolutePath(absolutePath: String, content: Array[Byte]): Unit = {
    CoreContext.dfs.create(new Path(absolutePath)).write(content)
  }

  def createAbsolutePath(absolutePath: String, content: String): Unit = {
    createAbsolutePath(absolutePath, content.getBytes)
  }

  def mkdirs(path: String): Boolean = {
    CoreContext.logger.debug("DFSRepository -> mkdirs : " + getFilePath(path))
    CoreContext.dfs.mkdirs(new Path(getFilePath(path)))
  }

  def rename(source: String, destination: String): Boolean = {
    CoreContext.logger.debug("DFSRepository -> rename : " + getFilePath(source) + " a " + getFilePath(destination))
    CoreContext.dfs.rename(new Path(getFilePath(source)), new Path(getFilePath(destination)))

  }

  def copyFromLocalToHDFS(source: String, destination: String) {
    val filePathHDFS = new Path(getFilePath(destination))
    // copyFromLocalFile(deleteFileFromSrc, ForceSave, PathSRC, filePathHDFS)
    CoreContext.dfs.copyFromLocalFile(true, true, new Path(source), filePathHDFS)

  }

  def listFiles(source: String, recursive: Boolean): Seq[String] = {

    val files = CoreContext.dfs.listFiles(new Path(getFilePath(source)), recursive)
    val result = new ListBuffer[String]()

    while (files.hasNext) {

      val file = files.next()
      val filename = removeFilePath(file.getPath.toString)
      result += filename
    }
    result
  }

  def getFilePath(path: String): String = {
    var prefix = ""
    if (CoreConfig.hdfs.prefix != null) {
      prefix = CoreConfig.hdfs.prefix
    }
    CoreConfig.hdfs.uri + prefix + path
  }

  private def removeFilePath(path: String): String = {
    var prefix = ""
    if (CoreConfig.hdfs.prefix != null) {
      prefix = CoreConfig.hdfs.prefix
    }
    path.replaceFirst("^" + CoreConfig.hdfs.uri + prefix, "")
  }

  /**
   * FileStatus de un fichero o directorio
   *
   * @param path - path al fichero
   * @return - FileStatus del fichero
   */
  private def listStatusFileOrDirectory(path: String): Array[FileStatus] = {
    CoreContext.logger.debug(s"DFSRepository -> listStatusFileOrDirectory: $path")
    CoreContext.dfs.listStatus(new Path(getFilePath(path)))
  }

  def listStatus(path: String, recursive: Boolean): ListBuffer[FileStatus] = {
    // Buffer con los ficheros encontrados
    val files = new ListBuffer[FileStatus]

    // ¿Es un directorio el path?
    if (recursive) {
      // Elementos del directorio
      val elements = CoreContext.dfs.listStatus(new Path(getFilePath(path)))

      // Navegamos por los elementos del directorio
      elements.foreach { element =>
        files.appendAll(listStatus(removeFilePath(element.getPath.toString), element.isDirectory))
      }
    } else {
      // Añadimos el FileStatus del fichero al ListBuffer
      files.appendAll(listStatusFileOrDirectory(path))
    }
    files
  }

  def isEmptyFile(path: String): Boolean = {
    val file = listStatus(path, recursive = false)
    if (file(0).getLen == 0)
      true
    else
      false
  }

  def textFile(path: String): RDD[String] = {
    CoreContext.logger.info("textFile - Leyendo fichero:" + getFilePath(path))
    CoreContext.sc.textFile(getFilePath(path))
  }

  def wholeTextFile(path: String): RDD[(String, String)] = {
    wholeTextFile(path,1)
  }

  def wholeTextFile(path: String, minPartitions: Int): RDD[(String, String)] = {
    CoreContext.logger.info("wholeTextFile - Leyendo fichero:" + getFilePath(path))
    CoreContext.sc.wholeTextFiles(getFilePath(path), minPartitions)
  }

  def readCsvAsDF(path: String, c: Type): DataFrame = {
    CoreContext.logger.info("Leyendo fichero:" + getFilePath(path))

    val resultDF = CoreContext.hiveContext.read.format("com.databricks.spark.csv")
      .option("header", "false")
      .option("inferSchema", "false")
      .schema(ScalaReflection.schemaFor(c).dataType.asInstanceOf[StructType])
      .load(getFilePath(path))
    resultDF

  }

  def readCsvAsDF(path: String, c: StructType): DataFrame = {
    readCsvAsDF(path, c, null)
  }

  def readCsvAsDF(path: String, c: StructType, delimiter: String): DataFrame = {

    readCsvAsDF(path, c, delimiter, header = false)
  }

  def readCsvAsDF(path: String, c: StructType, delimiter: String, header: Boolean): DataFrame = {

    readCsvAsDF(path, c, delimiter, header, null)

  }

  def readCsvAsDF(path: String, c: StructType, delimiter: String, header: Boolean, dateFormat: String): DataFrame = {
    CoreContext.logger.info("Leyendo fichero:" + getFilePath(path))

    var resultDF = CoreContext.hiveContext.read.format("com.databricks.spark.csv")
      .option("header", header.toString)
      .option("dateFormat", dateFormat)
      .option("inferSchema", "false")
      .schema(c)
    if (delimiter != null)
      resultDF = resultDF.option("delimiter", delimiter)
    resultDF.load(getFilePath(path))
  }

  def saveAsTextFiles(source: DStream[String], path: String) {
    source.saveAsTextFiles(getFilePath(path), "")
  }

  def saveAsTextFile(rdd: RDD[String], path: String) = {
    rdd.saveAsTextFile(getFilePath(path))
  }
}
