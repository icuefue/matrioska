package com.bluetab.matrioska.core.repositories

import org.json4s.reflect.ScalaType

trait JsonRepository {

  def serialize(obj: AnyRef): String

  def deserialize(json: String, c: ScalaType): Any

}
