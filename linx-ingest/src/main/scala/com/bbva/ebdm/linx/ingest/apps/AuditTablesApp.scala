package com.bbva.ebdm.linx.ingest.apps

import com.bbva.ebdm.linx.core.LinxApp
import com.bbva.ebdm.linx.core.conf.CoreServices

class AuditTablesApp extends LinxApp {

  override def run(args: Seq[String]) {

    CoreServices.auditoryService.auditTables(args.toArray)

  }

}
