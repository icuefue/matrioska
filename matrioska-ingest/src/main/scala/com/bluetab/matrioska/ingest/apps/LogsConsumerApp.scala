package com.bluetab.matrioska.ingest.apps

import org.apache.spark.streaming.dstream.DStream
import com.bluetab.matrioska.core.conf.CoreServices
import com.bluetab.matrioska.core.conf.CoreRepositories
import com.bluetab.matrioska.core.beans.MailRecipientTypes
import com.bluetab.matrioska.core.LinxStreamingApp
import com.bluetab.matrioska.core.beans.MailMessage
import com.bluetab.matrioska.core.conf.{CoreConfig, CoreServices}

class LogsConsumerApp extends LinxStreamingApp {

  override def run(args: Seq[String], dStream: DStream[String]) {

    dStream.foreachRDD(rdd => {

      CoreServices.governmentService.processLogs(rdd)

    })

  }

}
