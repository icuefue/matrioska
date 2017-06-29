package com.bluetab.matrioska.ingest.conf

import com.bluetab.matrioska.ingest.services._
import com.bluetab.matrioska.ingest.services.impl._



/**
 * Servicios disponibles
 */
object IngestServices {

  val gsdsService: GsdsService = new GsdsServiceImpl
  val informacionalService: InformacionalService = new InformacionalServiceImpl
  val tickHistoryService: TickHistoryService = new TickHistoryServiceImpl
  val tickHistory1Service: TickHistory1Service = new TickHistory1ServiceImpl
  val dataIngestionService: DataIngestionService = new DataIngestionServiceImpl
  val metadataService: MetadataService = new MetadataServiceImpl
  val prerawToRawService: PrerawToRawService = new PrerawToRawServiceImpl
  val prerawMaintenanceService: PrerawMaintenanceService = new PrerawMaintenanceServiceImpl
  val stagingToPrerawService: StagingToPrerawService = new StagingToPrerawServiceImpl
  val gmCalculatedDataService: GMCalculatedDataService = new GMCalculatedDataServiceImpl
  val evaService: EvaService = new EvaServiceImpl
  val netcashAnalyticsService: NetcashAnalyticsService = new NetcashAnalyticsServiceImpl
  val commonService: CommonService = new CommonServiceImpl

}
