package com.bluetab.matrioska.ingest.apps

import scala.annotation.migration
import org.apache.spark.streaming.dstream.DStream
import com.bluetab.matrioska.core.LinxStreamingApp
import com.bluetab.matrioska.core.conf.CoreServices
import com.bluetab.matrioska.ingest.conf.IngestServices

class DataIngestionApp extends LinxStreamingApp {

  def run(args: Seq[String], dStream: DStream[String]) {

    //Load Use Cases
    val useCases = dStream.context.sparkContext.broadcast(CoreServices.governmentService.findUseCases)

    //Map String Xml events DStream to Event Object Dstream
    val eventDStream = IngestServices.dataIngestionService.mapXmlToEvent(dStream)

    eventDStream.foreachRDD { rdd =>

      //Get distinct useCase
      val distinctUseCaseBasename = IngestServices.dataIngestionService.getDistinctUseCaseBasename(rdd)

      //Group Files by ItemPattern
      val itemFilesMap = IngestServices.dataIngestionService.groupFilesByItemPattern(useCases.value, distinctUseCaseBasename)

      for (item <- itemFilesMap.keys) {
        val files = itemFilesMap.get(item).get

        //Filter Rdd by Files
        val filteredRDD = IngestServices.dataIngestionService.filterRddByFiles(rdd, files)

        //Save As Table
        IngestServices.dataIngestionService.saveAsTable(item, filteredRDD)

      }
    }

  }

}
