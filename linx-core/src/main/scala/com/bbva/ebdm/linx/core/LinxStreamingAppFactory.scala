package com.bbva.ebdm.linx.core

/**
 * Factoría de app streaming
 */
object LinxStreamingAppFactory {

  /**
   * @param name - nombre de la app a lanzar
   * @return - clase de la app a lanzar
   */
  def apply(name: String) = {
    val c = Class.forName(name)

    c.newInstance() match {
      case c2: LinxStreamingApp => c2
      case _ => throw new ClassCastException("App not found")
    }

  }

}
