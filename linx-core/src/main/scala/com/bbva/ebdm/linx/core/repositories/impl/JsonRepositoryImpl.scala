package com.bbva.ebdm.linx.core.repositories.impl

import org.json4s._
import org.json4s.native.Serialization.{ read, write }
import org.json4s.native.JsonMethods._
import org.json4s.reflect.ScalaType
import com.bbva.ebdm.linx.core.repositories.JsonRepository
import com.bbva.ebdm.linx.core.repositories.JsonRepository

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
