package com.bbva.ebdm.linx.core.repositories

import com.bbva.ebdm.linx.core.beans.Metainfo

trait NavigatorRepository {

  def findMetainfoObject(desObject: String, sourceType: String, parentPath: String): Option[Metainfo]

  /**
    *
    * @param objectId - id del objeto a modificar
    * @param values - mapa de valores a propagar al objeto (Clave -> nombre del campo, Valor -> valor del campo)
    * @return
    */
  def putMetainfoObject(objectId: String, values: scala.collection.immutable.Map[String, String]): Boolean
}
