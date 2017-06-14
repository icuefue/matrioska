package com.bluetab.matrioska.core

import com.bluetab.matrioska.core.beans.{MailMessage, MailRecipientTypes}
import com.bluetab.matrioska.core.conf._
import com.bluetab.matrioska.core.exceptions.FatalException
import org.apache.commons.lang.exception.ExceptionUtils

/**
 * Lanzador de aplicaciones spark submit
 */
object LauncherApp extends App {

  /**
   * onStart
   */
  def onStart(): Unit = {

    if (CoreAppInfo.notFound) {
      throw new FatalException("App not found")
    } else {
      CoreServices.governmentService.logOnStart
    }
  }

  /**
   * onComplete
   */
  def onComplete(): Unit = {
    CoreConfig
    CoreServices.governmentService.logOnComplete
    closeImpalaConnection()
  }

  val parser = new scopt.OptionParser[LinxAppArgsConfig]("linx") {
    head("linx", "0.1.4")

    opt[String]('d', "plandate").action((x, c) =>
      c.copy(planDate = x)).text("(Obligatorio) planDate es la fecha de planificación del proceso. Ej: 20160505").required()
    opt[String]('l', "logLevel").action((x, c) =>
      c.copy(logLevel = x)).text("(Opcional) logLevel es el nivel de logs que se va a enviar a la cola Kafka")
    opt[Unit]('p', "pro").action((_, c) =>
      c.copy(isPro = true)).text("(Opcional) pro es el argumento necesario para ejecutar procesos en producción")
    opt[Unit]('f', "force").action((_, c) =>
      c.copy(force = true)).text("(Opcional) force es un flag opcional para poder ejecutar el proceso sin que esté dado de alta en la tabla de planificaciones")

    arg[String]("<App class> ").required.action((x, c) =>
      c.copy(appName = x)).text("(Obligatorio). Es la App que se va a ejecutar. Clase Ej: com.bluetab.matrioska.apps.TickHistory2App")

    arg[String]("<arg1> <arg2> <arg3>...").unbounded.optional().action((x, c) =>
      c.copy(appArgs = c.appArgs :+ x)).text("(Opcional) Son los argumentos opcionales de la app que se va a ejecutar")

    help("help").text("Instrucciones de uso")
  }

  /**
   * Carga de configuraciones
   */
  parser.parse(args, LinxAppArgsConfig()) match {

    // carga correcta de configuraciones
    case Some(config) =>
      LinxAppArgs.planDate = config.planDate
      LinxAppArgs.force = config.force
      LinxAppArgs.appName = config.appName
      LinxAppArgs.appArgs = config.appArgs
      LinxAppArgs.isPro = config.isPro
      LinxAppArgs.logLevel = config.logLevel
      LinxAppArgs.isApp = true
      LinxAppArgs.user = System.getenv().get("USER")

      try {
        onStart()
        // Obtenemos la clase de app a lanzar y la arrancamos. Se le pasan sus argumentos
        LinxAppFactory(LinxAppArgs.appName).run(LinxAppArgs.appArgs)
        onComplete()
      } catch {
        case ex: Throwable =>
          if (CoreConfig.environment == "PRO") {
            val mailMessage = new MailMessage
            mailMessage.addRecipient(MailRecipientTypes.To, "soporte_data_analytics_cib@bbva.com")
            mailMessage.subject = s"Error app Linx: ${LinxAppArgs.appName}"
            mailMessage.text = "Se han producido errores en la applicación con Id: " + CoreAppInfo.uuid +
              "\n\n" + ExceptionUtils.getFullStackTrace(ex)
            CoreRepositories.mailRepository.sendMail(mailMessage)
          }
          CoreServices.governmentService.logOnError(ex)
          closeImpalaConnection()
          throw ex
      }
    // carga errónea de configuraciones
    case None =>
    //EbdmServices.governmentService.logOnStart
    //EbdmServices.governmentService.logOnError(new FatalException("No se han pasado correctamente los parámetros de Linx"))
  }

  /**
   * Cierre de la conexión de Impala
   */
  def closeImpalaConnection() = {

    CoreContext.impalaConnection match {
      case Some(connection) => if (connection != null) connection.close()
      case None => CoreContext.logger.info("impalaConnection -> null")
    }
  }
}
