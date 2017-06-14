package com.bbva.ebdm.linx.core


/**
  * Factoría de app submit
  */
object LinxAppFactory {

  /**
    * @param name - nombre de la app a lanzar
    * @return - clase de la app a lanzar
    */
  def apply(name: String) = {
    val c = Class.forName(name)
    c.newInstance() match {
      case c2: LinxApp => c2
      case _ => throw new ClassCastException("App not found")
    }
  }
}
