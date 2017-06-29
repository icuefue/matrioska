package com.bluetab.matrioska.core.repositories

/**
  * Created by xe54068 on 23/01/2017.
  */
trait SolrRepository {

  /**
    * Borrado de una colección
    *
    * @param name - nombre de la colección a borrar
    */
  def deleteCollection(name: String): Unit

  /**
    * Creación de una colección
    *
    * @param name - nombre de la colección a borrar
    */
  def createCollection(name: String): Unit

  /**
    * Volcado del contenido del diccionario a una colección
    *
    * @param name - nombre de la colección
    */
  def loadDictionaryToCollection(name: String): Unit

}
