package com.bluetab.matrioska.ingest.apps

import com.bluetab.matrioska.core.LinxApp
import com.bluetab.matrioska.core.conf.CoreRepositories

class PruebaHBaseApp extends LinxApp {

  override def run(args: Seq[String]) {

    val schema = "ebdm"
    val table = "stats_table"
    val content = ("tablucha", scala.collection.immutable.Map(("stats", scala.collection.immutable.Map(("count", "22")))))

    CoreRepositories.hBaseRepository.write(schema, table, content)

  }

}
