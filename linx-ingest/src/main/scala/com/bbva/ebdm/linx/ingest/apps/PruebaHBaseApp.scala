package com.bbva.ebdm.linx.ingest.apps

import com.bbva.ebdm.linx.core.LinxApp
import com.bbva.ebdm.linx.core.conf.CoreRepositories

class PruebaHBaseApp extends LinxApp {

  override def run(args: Seq[String]) {

    val schema = "ebdm"
    val table = "stats_table"
    val content = ("tablucha", scala.collection.immutable.Map(("stats", scala.collection.immutable.Map(("count", "22")))))

    CoreRepositories.hBaseRepository.write(schema, table, content)

  }

}
