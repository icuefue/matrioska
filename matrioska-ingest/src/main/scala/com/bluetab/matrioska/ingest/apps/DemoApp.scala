package com.bluetab.matrioska.ingest.apps

import org.apache.spark.sql.functions._
import com.bluetab.matrioska.core.conf.CoreContext
import com.bluetab.matrioska.core.conf.CoreRepositories
import com.bluetab.matrioska.core.beans.MailRecipientTypes.MailRecipientType
import com.bluetab.matrioska.core.beans.MailRecipientTypes
import org.apache.spark.rdd.RDD
import com.bluetab.matrioska.core.LinxApp
import com.bluetab.matrioska.core.beans.MailMessage
import com.bluetab.matrioska.core.conf.{CoreAppInfo, CoreRepositories}
import com.bluetab.matrioska.ingest.conf.IngestServices

class DemoApp extends LinxApp {

  override def run(args: Seq[String]) {

    CoreContext.logger.info("La malla es: " + CoreAppInfo.malla)
    CoreContext.logger.info("El job es: " + CoreAppInfo.job)
    CoreContext.logger.info("La capa es: " + CoreAppInfo.capa)
    CoreContext.logger.info("El name es: " + CoreAppInfo.name)

    CoreContext.logger.info("El name es: " + CoreRepositories.dfsRepository.getFilePath("/hola/caracola"))

//    val sources = IngestServices.metadataService.loadSources
//    sources.values.foreach { x => 
//      println("\n\n\nSources:\n" + x.id + "\n\n\n")
//      x.masks.foreach { x =>  
//        println (x.mask)
//        x.faultToleranceTests.foreach { x => println("faultTolerance:" + x.toString()) }
//      }
//    }

  }
}