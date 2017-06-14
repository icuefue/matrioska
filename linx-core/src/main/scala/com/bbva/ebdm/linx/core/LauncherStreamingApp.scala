package com.bbva.ebdm.linx.core

import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.kafka.KafkaUtils

import com.bbva.ebdm.linx.core.beans.MailMessage
import com.bbva.ebdm.linx.core.beans.MailRecipientTypes
import com.bbva.ebdm.linx.core.conf.CoreRepositories
import com.bbva.ebdm.linx.core.conf.CoreConfig
import com.bbva.ebdm.linx.core.conf.CoreAppInfo
import com.bbva.ebdm.linx.core.conf.CoreServices
import com.bbva.ebdm.linx.core.conf.CoreContext
import org.apache.commons.lang.exception.ExceptionUtils
import kafka.serializer.StringDecoder
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.spark.storage.StorageLevel

/**
 * Lanzador de aplicaciones spark streaming
 */
object LauncherStreamingApp extends App {

  def functionToCreateContext(): StreamingContext = {

    val zkQuorum = CoreConfig.kafka.zqquorum
    val brokerList = CoreConfig.kafka.brokerlist

    val topic = LinxAppArgs.topic
    val group = LinxAppArgs.group
    val secProtocol = "PLAINTEXTSASL"

    val topicMap = Map(topic -> 1)
    
    //val kafkaParams = Map("metadata.broker.list" -> brokerList,
    val kafkaParams = Map("bootstrap.servers" -> brokerList,
                          "group.id"             -> group,
                          "security.protocol" -> secProtocol)

    val events = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](CoreContext.ssc.asInstanceOf[org.apache.spark.streaming.StreamingContext], kafkaParams, topicMap, StorageLevel.MEMORY_AND_DISK_SER_2).map(_._2)

    LinxStreamingAppFactory(LinxAppArgs.appName).run(LinxAppArgs.appArgs, events)

    CoreContext.ssc
  }

  val parser = new scopt.OptionParser[LinxStreamingAppArgsConfig]("linx") {
    head("linx")
    opt[Unit]('p', "pro").action((_, c) =>
      c.copy(isPro = true)).text("(Opcional) pro es el argumento necesario para ejecutar procesos en producción")
    opt[String]('l', "logLevel").action((x, c) =>
      c.copy(logLevel = x)).text("(Opcional) logLevel es el nivel de logs que se va a enviar a la cola Kafka")
    opt[String]('t', "topic").action((x, c) =>
      c.copy(topic = x)).text("(Obligatorio) topic es la topic kafka que se leerá. Ej: ebdm-logs").required()
    opt[String]('g', "group").action((x, c) =>
      c.copy(group = x)).text("(Obligatorio) Es el grupo que leerá la cola kafka. Ej: core").required()
    opt[Int]('i', "interval").action((x, c) =>
      c.copy(interval = x)).text("(Obligatorio) Intervalo de lectura de la cola kafka. Ej: 300").required()
    help("help").text("Instrucciones de uso")

    arg[String]("<App class> ").required.action((x, c) =>
      c.copy(appName = x)).text("(Obligatorio). Es la App que se va a ejecutar. Clase Ej: com.bbva.ebdm.linx.apps.LogsStreamingApp")
    arg[String]("<arg1> <arg2> <arg3>...").unbounded.optional().action((x, c) =>
      c.copy(appArgs = c.appArgs :+ x)).text("(Opcional) Son los argumentos opcionales de la app que se va a ejecutar")

  }

  parser.parse(args, LinxStreamingAppArgsConfig()) match {
    case Some(config) =>
      LinxAppArgs.appName = config.appName
      LinxAppArgs.topic = config.topic
      LinxAppArgs.group = config.group
      LinxAppArgs.interval = config.interval
      LinxAppArgs.isPro = config.isPro
      LinxAppArgs.appArgs = config.appArgs
      LinxAppArgs.logLevel = config.logLevel
      LinxAppArgs.user = System.getenv().get("USER")

      try {
        val checkpointDir = CoreConfig.kafka.checkpointDir

        val context = StreamingContext.getOrCreate(checkpointDir, functionToCreateContext _)
        context.start()
        context.awaitTermination()
      } catch {
        case ex: Throwable =>
          if (CoreConfig.environment == "PRO") {
            val mailMessage = new MailMessage
            mailMessage.addRecipient(MailRecipientTypes.To, "soporte_data_analytics_cib@bbva.com")
            mailMessage.subject = s"Error Streaming App Linx: ${LinxAppArgs.appName}"
            mailMessage.text = "Se han producido errores en la applicación con Id: " + CoreAppInfo.uuid +
              "\n\n" + ExceptionUtils.getFullStackTrace(ex)
            CoreRepositories.mailRepository.sendMail(mailMessage)
          }
          CoreServices.governmentService.logOnError(ex)
          throw ex;
      }
    case None =>
    //EbdmServices.governmentService.logOnStart
    //EbdmServices.governmentService.logOnError(new FatalException("No se han pasado correctamente los parámetros de Linx"))
  }

}


