package com.bbva.ebdm.linx.ingest.apps

import java.io.FileInputStream
import java.io.BufferedWriter
import java.io.OutputStreamWriter
import java.io.BufferedReader
import java.nio.charset.Charset
import java.io.FileOutputStream
import java.io.BufferedInputStream
import java.io.InputStreamReader
import org.mozilla.universalchardet.UniversalDetector
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object LocalApp extends App {

  val lista = Seq(("hola_*_adios", 1), ("hola_pepe_adios", 1), ("hola_luis_adios", 1))

  private val conf = new SparkConf().setAppName("oe")
  val sc =  new SparkContext(conf)
  
  val rdd = sc.parallelize(lista)
   

  val auditRdd = rdd.reduceByKey(_+_)
  auditRdd.collect.foreach(println);

  
  
  /*
  val fichero1 = "c:/Users/xe58268/ESKYGUENSP_NETCASH_20170125_001.dat"
  val fichero2 = "c:/Users/xe58268/ESKYGUENSP_NETCASH_20170125_001.dat.utf8"
  val fichero3 = "c:/Users/xe58268/ESKYGUENSP_NETCASH_20170124_001.dat_PROCESADO"

  val charset1 = detectFileCharset(fichero1)
  println(charset1.name())
  
  convertFile(fichero1, fichero2, charset1, Charset.forName("UTF-8"))
  
  val charset2 = detectFileCharset(fichero2)
  println(charset2.name())
*/

  def detectFileCharset(filepath: String): Charset = {
    val ud = new UniversalDetector(null)
    val fis = new FileInputStream(filepath)
    val buffer = new Array[Byte](16384)
    var nread = 0
    while ({
      nread = fis.read(buffer);
      nread
    } > 0 && !ud.isDone) {
      ud.handleData(buffer, 0, nread)
    }
    ud.dataEnd()

    val encoding = ud.getDetectedCharset
    if (encoding != null) {

      Charset.forName(encoding)
    } else {

      Charset.defaultCharset
    }
  }

  def convertFile(originalFile: String, newFile: String, originalCharset: Charset, newCharset: Charset) {

    val br = new BufferedReader(new InputStreamReader(new FileInputStream(originalFile), originalCharset))
    val bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(newFile), newCharset));
    val buffer = new Array[Char](16384)
    var read = 0;

    while ({ read = br.read(buffer); read } != -1) {
      println(read)
      bw.write(buffer, 0, read)
    }
  }

}
