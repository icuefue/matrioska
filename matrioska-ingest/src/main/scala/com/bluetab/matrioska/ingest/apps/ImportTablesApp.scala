package com.bluetab.matrioska.ingest.apps

import com.bluetab.matrioska.core.conf.CoreContext
import com.bluetab.matrioska.core.LinxApp
import com.bluetab.matrioska.core.exceptions.FatalException
import com.bluetab.matrioska.ingest.conf.IngestServices

/**
  * App de importación de tablas de un caso de uso
  *
  * Los casos de uso están listados en la tabla rd_ebdmgv.t_ebdmgv11_usecases
  */
class ImportTablesApp extends LinxApp {

  override def run(args: Seq[String]) {

    if (args.nonEmpty) {
      CoreContext.logger.debug(s"Caso de uso a importar sus tablas -> ${args.head}")
      IngestServices.commonService.importTables(args.head)
    } else {
      throw new FatalException("ImportTablesApp requiere un parámetro(Caso de uso)")
    }

  }

}
