package com.bluetab.matrioska.core.conf

import java.io.IOException
import java.security.PrivilegedExceptionAction
import java.security.cert.X509Certificate
import java.sql.DriverManager
import javax.net.ssl.SSLContext

import com.bluetab.matrioska.core.LinxAppArgs
import com.bluetab.matrioska.core.beans.LogPatternLayout
import com.cloudera.sqoop.SqoopOptions
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.apache.hadoop.security.UserGroupInformation
import org.apache.http.ssl.{SSLContexts, TrustStrategy}
import org.apache.kafka.log4jappender.KafkaLog4jAppender
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.slf4j.LoggerFactory
import spray.io.ClientSSLEngineProvider
import org.apache.log4j.FileAppender

@transient object CoreContext {  

//  val appender = new KafkaLog4jAppender //configure the appender
//  private val pattern = "%X{header}\t%d{yyyy/MM/dd HH:mm:ss}\t%X{uuid}\t%X{estado}\t%X{malla}\t%X{job}\t%X{name}\t%X{capa}\t%X{plandate}\t%-5p\t\t%m";
//  private val patternLayout = new LogPatternLayout()
//  patternLayout.setConversionPattern(pattern)
//  appender.setLayout(patternLayout)
//  appender.setThreshold(Level.toLevel(LinxAppArgs.logLevel))
//  appender.setTopic(CoreConfig.kafka.topicLogs)
//  appender.setBrokerList(CoreConfig.kafka.brokerlist)
//  appender.setSyncSend(true)
//  appender.setSecurityProtocol("SASL_PLAINTEXT")
//  appender.setSslTruststorePassword("KerbKeyTrusteePassword")
//  appender.setSslTruststoreLocation("KerbKeyTrusteeLocation")
//  appender.activateOptions()
//
  val appender = new FileAppender
  private val pattern = "%X{header}\t%d{yyyy/MM/dd HH:mm:ss}\t%X{uuid}\t%X{estado}\t%X{malla}\t%X{job}\t%X{name}\t%X{capa}\t%X{plandate}\t%-5p\t%m%n";
  private val patternLayout = new LogPatternLayout()
  patternLayout.setConversionPattern(pattern)
  appender.setLayout(patternLayout)
  appender.setName("FileLogger");
  appender.setFile("salida.log");
  appender.setThreshold(Level.DEBUG);
  appender.setAppend(false);
  appender.activateOptions()
  Logger.getLogger("com.bluetab.matrioska").addAppender(appender)
//  Logger.getLogger("com.bluetab.matrioska")

  private val conf = new SparkConf().setAppName(LinxAppArgs.appName)

  val ssc: StreamingContext = if (!LinxAppArgs.isApp) {
    new StreamingContext(conf, Seconds(LinxAppArgs.interval))
  } else { null }

  val sc = if (LinxAppArgs.isApp) new SparkContext(conf) else ssc.sparkContext

  val logger = LoggerFactory.getLogger(LinxAppArgs.appName)

//  val ugi = UserGroupInformation.loginUserFromKeytabAndReturnUGI(CoreConfig.kerberos.user, "./.keytab");

  var hBaseConnection: Connection = null
//  CoreContext.ugi.doAs(new PrivilegedExceptionAction[Void]() {
//    @throws(classOf[IOException])
//    def run: Void = {
//      hBaseConnection = ConnectionFactory.createConnection()
//      return null
//    }
//  });

  val hadoopConf = new org.apache.hadoop.conf.Configuration()

//  hadoopConf.set("hadoop.security.authentication", "kerberos");

//  hadoopConf.set("hadoop.security.authorization", "true");

//  UserGroupInformation.setConfiguration(hadoopConf);

  val dfs = org.apache.hadoop.fs.FileSystem.get(new java.net.URI(CoreConfig.hdfs.uri), hadoopConf)

  val sqoopOptions = new SqoopOptions();
  sqoopOptions.setConf(hadoopConf);

  val hiveContext = new HiveContext(sc)
  hiveContext.setConf("hive.exec.dynamic.partition", "true")
  hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

  val sqlContext = new SQLContext(sc)

  //Definimos variables implicitas SSLContext, ClientSSLEngineProvider  
  //que nos permiten realizar llamadas REST a servidores securizados
  // La trustStrategy no es segura y debería tirar de un certificado introducido en un contenedor JKS
  implicit val sslContext: SSLContext = {
    val trustStrategy = new TrustStrategy() {
      override def isTrusted(chain: Array[X509Certificate], authType: String) = true
    }
    SSLContexts.custom().loadTrustMaterial(null, trustStrategy).build()
  }

  implicit val clientSSLEngineProvider = ClientSSLEngineProvider { engine =>
    engine.setEnabledCipherSuites(Array("TLS_RSA_WITH_AES_256_CBC_SHA"))
    engine.setEnabledProtocols(Array("SSLv3", "TLSv1"))
    engine
  }

  var impalaConnection: Option[java.sql.Connection] = None

//  CoreContext.ugi.doAs(new PrivilegedExceptionAction[Void]() {
//    @throws(classOf[IOException])
//    def run: Void = {
//      Class.forName(CoreConfig.hive.driver).newInstance()
//      impalaConnection = Some(DriverManager.getConnection(CoreConfig.impala.uri))
//      return null
//    }
//  });

}
