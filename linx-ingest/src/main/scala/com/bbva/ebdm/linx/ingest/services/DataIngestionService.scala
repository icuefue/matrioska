package com.bbva.ebdm.linx.ingest.services

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import com.bbva.ebdm.linx.core.beans.Event
import com.bbva.ebdm.linx.core.beans.UseCase
import com.bbva.ebdm.linx.core.beans.Item

trait DataIngestionService {

  def mapXmlToEvent(dStream: DStream[String]): DStream[Event]

  def getDistinctUseCaseBasename(rdd: RDD[Event]): Array[(String, String)]

  def groupFilesByItemPattern(useCases: Map[String, UseCase],
    useCaseFile: Array[(String, String)]): Map[Item, ArrayBuffer[(String, String)]]

  def filterRddByFiles(rdd: RDD[Event], files: ArrayBuffer[(String, String)]): RDD[Event]

  def saveAsTable(item: Item, rdd: RDD[Event])

}
