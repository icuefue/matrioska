package com.bluetab.matrioska.core.repositories.impl

import com.bluetab.matrioska.core.repositories.JsonRepository
import org.json4s._
import org.json4s.native.JsonMethods._
import org.json4s.native.Serialization.write
import org.json4s.reflect.ScalaType

class JsonRepositoryImpl extends JsonRepository {

  def serialize(obj: AnyRef): String = {

    implicit val formats = native.Serialization.formats(NoTypeHints)

    write(obj)

  }

  def deserialize(json: String, c: ScalaType) : Any = {
    implicit val formats = native.Serialization.formats(NoTypeHints)
    Extraction.extract(parse(json), c)

  }
}
