package com.bluetab.matrioska.ingest.apps

import com.bluetab.matrioska.core.LinxApp
import com.bluetab.matrioska.core.conf.CoreServices


class AuditTablesApp extends LinxApp {

  override def run(args: Seq[String]) {

    CoreServices.auditoryService.auditTables(args.toArray)

  }

}
