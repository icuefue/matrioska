package com.bbva.ebdm.linx.ingest.apps

import com.bbva.ebdm.linx.core.LinxStreamingApp
import org.apache.spark.streaming.dstream.DStream
import com.bbva.ebdm.linx.core.conf.CoreServices
import com.bbva.ebdm.linx.core.beans.MailMessage
import com.bbva.ebdm.linx.core.conf.CoreRepositories
import com.bbva.ebdm.linx.core.conf.CoreConfig
import com.bbva.ebdm.linx.core.beans.MailRecipientTypes

class LogsConsumerApp extends LinxStreamingApp {

  override def run(args: Seq[String], dStream: DStream[String]) {

    dStream.foreachRDD(rdd => {

      CoreServices.governmentService.processLogs(rdd)

    })

  }

}
