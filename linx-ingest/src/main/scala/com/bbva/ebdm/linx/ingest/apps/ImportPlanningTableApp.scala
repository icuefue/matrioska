package com.bbva.ebdm.linx.ingest.apps

import com.bbva.ebdm.linx.core.LinxApp
import com.bbva.ebdm.linx.ingest.conf.IngestServices

/**
  * Created by xe54068 on 30/01/2017.
  */
class ImportPlanningTableApp extends LinxApp {

  override def run(args: Seq[String]): Unit = {

    // Importamos la tabla del caso de uso "planning"
    IngestServices.commonService.importTables(s"planning")

  }


}
