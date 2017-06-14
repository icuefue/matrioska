package com.bbva.ebdm.linx.ingest.services.impl

import org.apache.spark.streaming.dstream.DStream
import org.joda.time.format.DateTimeFormat
import org.joda.time.DateTime
import com.bbva.ebdm.linx.core.beans.Event
import org.apache.spark.rdd.RDD
import scala.collection.mutable.ArrayBuffer
import scala.util.control.Breaks.break
import scala.util.control.Breaks.breakable
import scala.collection.mutable.Map
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.SaveMode
import org.json4s.reflect.Reflector
import com.bbva.ebdm.linx.ingest.services.DataIngestionService
import com.bbva.ebdm.linx.core.conf.CoreRepositories
import com.bbva.ebdm.linx.core.beans.Item
import com.bbva.ebdm.linx.core.beans.UseCase

class DataIngestionServiceImpl extends DataIngestionService {

  def mapXmlToEvent(dStream: DStream[String]): DStream[Event] = {
    val result = dStream.map { x =>
      var event = CoreRepositories.jsonRepository.deserialize(x, Reflector.scalaTypeOf[Event])
      event match {
        case event: Event =>
          event
        case _ => throw new ClassCastException
      }
    }
    result
  }

  def getDistinctUseCaseBasename(rdd: RDD[Event]): Array[(String, String)] = {

    rdd.map(x => (x.headers.use_case, x.headers.basename)).distinct.collect()

  }

  def groupFilesByItemPattern(useCases: Map[String, UseCase],
    distinctUseCaseBasename: Array[(String, String)]): Map[Item, ArrayBuffer[(String, String)]] = {
    val itemFilesMap = Map[Item, ArrayBuffer[(String, String)]]();
    for (useCaseBasename <- distinctUseCaseBasename) {
      val useCase = useCases.get(useCaseBasename._1)
      breakable {
        for (item <- useCase.get.items) {
          if (useCaseBasename._2.matches(item.mask)) {
            if (itemFilesMap.get(item).isEmpty)
              itemFilesMap.put(item, ArrayBuffer(useCaseBasename))
            else {
              itemFilesMap.get(item).get += useCaseBasename
            }
            break;
          }
        }
      }
    }
    itemFilesMap
  }

  def filterRddByFiles(rdd: RDD[Event], files: ArrayBuffer[(String, String)]): RDD[Event] = {

    rdd.filter { f =>
      var result = false
      breakable {
        for (file <- files) {
          if (file._2 == f.headers.basename) {
            result = true
            break
          }
        }
      }
      result
    }

  }

  def saveAsTable(item: Item, rdd: RDD[Event]) = {

    val hiveContext = new HiveContext(rdd.sparkContext)
    import hiveContext.implicits._
    import hiveContext.sql
    hiveContext.setConf("hive.exec.dynamic.partition", "true")
    hiveContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    val rddDF = rdd.map({x : Event => (x.body, x.headers.basename, x.headers.date, x.getPartitionDate)}).toDF("body", "source_name", "incoming_date", "partition_incoming_date")
    rddDF.write.mode(SaveMode.Append).partitionBy("partition_incoming_date").saveAsTable(item.destination.destination)

  }

}
