package com.bluetab.matrioska.ingest.services

import org.apache.spark.rdd.RDD
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Map
import com.bluetab.matrioska.core.beans.Event
import com.bluetab.matrioska.core.beans.{Item, UseCase}

trait DataIngestionService {

  def mapXmlToEvent(dStream: DStream[String]): DStream[Event]

  def getDistinctUseCaseBasename(rdd: RDD[Event]): Array[(String, String)]

  def groupFilesByItemPattern(useCases: Map[String, UseCase],
    useCaseFile: Array[(String, String)]): Map[Item, ArrayBuffer[(String, String)]]

  def filterRddByFiles(rdd: RDD[Event], files: ArrayBuffer[(String, String)]): RDD[Event]

  def saveAsTable(item: Item, rdd: RDD[Event])

}
