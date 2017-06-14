package com.bluetab.matrioska.core.conf

import java.util.{HashMap, Map}

import com.bluetab.matrioska.core.LinxAppArgs
import org.yaml.snakeyaml.Yaml
import org.yaml.snakeyaml.constructor.Constructor

import scala.beans.BeanProperty

object CoreConfig {

  private val yaml = new Yaml(new Constructor(classOf[CoreConfig]))
  val configPath = if (LinxAppArgs.isPro) "/pro.yml" else "/dev.yml"
  var file = scala.io.Source.fromInputStream(getClass.getResourceAsStream(configPath)).mkString
  file = file.replaceAll("%\\{user\\}", LinxAppArgs.user)
  private val e = yaml.load(file).asInstanceOf[CoreConfig]
  val hdfs = e.hdfs
  val environment = e.environment
  val hive = e.hive
  val hbase = e.hbase
  val impala = e.impala
  val kafka = e.kafka
  val rdbms = e.rdbms
  val navigator = e.navigator
  val kerberos = e.kerberos
  val localfilesystempath = e.localfilesystempath
  val mail = e.mail
  val solr = e.solr
}

class CoreConfig {
  @BeanProperty var environment: String = ""
  @BeanProperty var hdfs: HdfsConfig = new HdfsConfig
  @BeanProperty var hive: HiveConfig = new HiveConfig
  @BeanProperty var impala: ImpalaConfig = new ImpalaConfig
  @BeanProperty var hbase: HBaseConfig = new HBaseConfig
  @BeanProperty var kafka: KafkaConfig = new KafkaConfig
  @BeanProperty var rdbms: Map[String, Map[String, String]] = new HashMap
  @BeanProperty var navigator: NavigatorConfig = new NavigatorConfig
  @BeanProperty var kerberos: KerberosConfig = new KerberosConfig
  @BeanProperty var localfilesystempath: LocalfilesystempathConfig = new LocalfilesystempathConfig
  @BeanProperty var mail: MailConfig = new MailConfig
  @BeanProperty var solr: SolrConfig = new SolrConfig

  override def toString: String = s"environment=$environment, hdfs={$hdfs}, " +
    s"hive={$hive}, impala={$impala}, hbase={$hbase}, kafka={$kafka}, rdbms={$rdbms}, " +
    s"navigator={$navigator}, kerberos={$kerberos}, localfilesystempath={$localfilesystempath}" +
    s"mail={$mail}, solr={$solr},"
}

class HdfsConfig {
  @BeanProperty var uri: String = ""
  @BeanProperty var prefix: String = ""

  override def toString: String = s"$uri, $prefix"

}

class ImpalaConfig {
  @BeanProperty var uri: String = ""
  override def toString: String = s"$uri"
}

class HiveConfig {
  @BeanProperty var driver: String = ""
  @BeanProperty var schemas: Map[String, String] = new HashMap[String, String]()
  override def toString: String = s"$driver, $schemas"
}

class HBaseConfig {
  @BeanProperty var schemas: Map[String, String] = new HashMap[String, String]()

  override def toString: String = s"$schemas"
}

class KafkaConfig {
  @BeanProperty var zqquorum: String = ""
  @BeanProperty var brokerlist: String = ""
  @BeanProperty var topicLogs: String = ""
  @BeanProperty var checkpointDir: String = ""

  override def toString: String = s"$zqquorum, $brokerlist, $topicLogs, $checkpointDir"
}

class KerberosConfig {
  @BeanProperty var user: String = ""
  @BeanProperty var keytab: String = ""

  override def toString: String = s"$user, $keytab"
}

class LocalfilesystempathConfig {
  @BeanProperty var staging: String = ""
  @BeanProperty var dat: String = ""

  override def toString: String = s"$staging, $dat"
}

class NavigatorConfig {
  @BeanProperty var uri: String = ""
  @BeanProperty var host: String = ""
  @BeanProperty var port: Int = 0
  @BeanProperty var username: String = ""
  @BeanProperty var password: String = ""

  override def toString: String = s"$uri, $host, $port, $username, $password"
}

class SolrConfig {
  @BeanProperty var uri: String = ""
  @BeanProperty var dictionaryCollection: String = ""

  override def toString: String = s"$uri, $dictionaryCollection"
}

class MailConfig {

  @BeanProperty var script: String = ""
  override def toString: String = s"$script"
}

