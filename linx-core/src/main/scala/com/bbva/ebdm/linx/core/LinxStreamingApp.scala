package com.bbva.ebdm.linx.core

import org.apache.spark.streaming.dstream.DStream

/**
  *  Trait de una App spark streaming
  */
trait LinxStreamingApp {

  /**
    * Run de la apliación
    *
    * @param args - argumentos para la app
    */
  def run(args : Seq[String], dStream: DStream[String])
}
