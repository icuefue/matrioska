package com.bluetab.matrioska.core

/**
 *  Trait de una App spark submit
 */
trait LinxApp {

  /**
   * Run de la apliación
   *
   * @param args - argumentos para la app
   */
  def run(args: Seq[String])

}
