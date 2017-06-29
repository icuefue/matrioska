package com.bluetab.matrioska.core.repositories.impl

import java.io._
import java.nio.charset.Charset

import com.bluetab.matrioska.core.conf.CoreContext
import com.bluetab.matrioska.core.repositories.FSRepository
import org.mozilla.universalchardet.UniversalDetector

import scala.util.matching.Regex

class FSRepositoryImpl extends FSRepository {

  def listFiles(f: File): Array[File] = {
    listFiles(f, recursive = false)
  }

  def listFiles(f: File, recursive: Boolean): Array[File] = {
    listFiles(f, recursive, excludeDirectories = false)
  }

  def listFiles(f: File, recursive: Boolean, excludeDirectories: Boolean): Array[File] = {

    listFiles(f, ".*".r, recursive, excludeDirectories)
  }

  def listFiles(f: File, r: Regex, recursive: Boolean, excludeDirectories: Boolean): Array[File] = {
    CoreContext.logger.info("Listando ficheros del directorio: " + f.getAbsolutePath)

    var good = Array[File]()
    val these = f.listFiles
    if (these != null) {
      good = these.filter(f => r.findFirstIn(f.getName).isDefined)
      if (recursive) {
        good = good ++ these.filter(_.isDirectory).flatMap(listFiles(_, r, recursive, excludeDirectories))
      }
      if (excludeDirectories) {
        good = good.filter(x => !x.isDirectory)
      }

    }
    good
  }

  def renameFile(f: File, sufix: String): File = {
    val fileSufix = new File(f + sufix)
    f.renameTo(fileSufix)
    fileSufix
  }

  def deleteSufix(f: File, sufix: String): File = {
    val filePathLocalS = new File(f.toString.dropRight(sufix.length()))
    f.renameTo(filePathLocalS)
    filePathLocalS
  }

  override def createFileWithContent(filePath: String, content: Array[Char]): Unit = {
    val fileWriter = new FileWriter(filePath)
    fileWriter.write(content)
    fileWriter.flush()
    fileWriter.close()
  }

  override def detectFileCharset(filepath: String): Charset = {
    val ud = new UniversalDetector(null)
    val fis = new FileInputStream(filepath)
    val buffer = new Array[Byte](1048576)
    var nread = 0
    while ( {
      nread = fis.read(buffer)
      nread
    } > 0 && !ud.isDone) {
      ud.handleData(buffer, 0, nread)
    }
    ud.dataEnd()

    val encoding = ud.getDetectedCharset
    if (encoding != null) {
      CoreContext.logger.info(s"$filepath => $encoding")
      Charset.forName(encoding)
    } else {
      CoreContext.logger.info(s"$filepath => enconding no definido, asignado el de por defecto")
      val charset = Charset.forName("windows-1252")
      CoreContext.logger.info(s"Encoding por defecto: ${charset.toString}")
      charset
      //Charset.defaultCharset
    }
  }

  override def convertFile(originalFile: String, newFile: String, originalCharset: Charset, newCharset: Charset) {

    val br = new BufferedReader(new InputStreamReader(new FileInputStream(originalFile), originalCharset))
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(newFile), newCharset))
    val buffer = new Array[Char](16384)
    var read = 0

    while ({ read = br.read(buffer); read } != -1) {
      bw.write(buffer, 0, read)
    }
    bw.flush()
    bw.close()
    CoreContext.logger.info(s"Se convierte el fichero $originalFile de $originalCharset a $newCharset en $newFile")
  }

  override def deleteFile(filepath: String): Boolean = {
    val file = new File(filepath)
    if(file.isFile) {
      CoreContext.logger.info(s"Borrando ${file.getAbsolutePath}")
      file.delete
    }
    else {
      CoreContext.logger.info(s"La ruta ${file.getAbsolutePath} no es un fichero")
      false
    }
  }
}
