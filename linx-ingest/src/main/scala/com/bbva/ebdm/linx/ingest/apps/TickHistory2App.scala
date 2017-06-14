package com.bbva.ebdm.linx.ingest.apps

import com.bbva.ebdm.linx.core.LinxApp
import com.bbva.ebdm.linx.ingest.conf.IngestServices

class TickHistory2App extends LinxApp {

  override def run(args: Seq[String]) {

    // Recuperamos la tabla de eventos con la información sobre los splits de las acciones
    val events = IngestServices.gsdsService.loadEvents

    // Recuperamos la tabla de dividendos con la información sobre los dividendos
    val dividends = IngestServices.gsdsService.loadDividends

    //Recuperamos la tabla de tiempos
    val dates = IngestServices.informacionalService.loadTimeInfo

    //Recuperamos la tabla EndOfDays con el histórico del valor de las acciones
    val endOfDays = IngestServices.gsdsService.loadEndOfDays

    //Realizamos las transformaciones definidas para la tabla de splits
    val transformedEvents = IngestServices.tickHistoryService.transformEvents(events)

    //Realizamos las transformaciones definidas para la tabla de dividendos
    val transformedDividends = IngestServices.tickHistoryService.transformDividends(dividends)

    //Realizamos un join de las tablas para obtener una tabla con fecha, accion, valor, split, dividendo
    val joinedData = IngestServices.tickHistoryService.joinEventsAndDividendsByDateAndRic(dates, endOfDays, transformedEvents, transformedDividends)

    //Calculamos el valor corrector de cada acción por fecha
    val resultado = IngestServices.tickHistoryService.calculateAdjustmentFactorData(joinedData)

    //Guardamos la tabla en hdfs
    IngestServices.tickHistoryService.saveAdjustmentFactorData(resultado)
  }

}
