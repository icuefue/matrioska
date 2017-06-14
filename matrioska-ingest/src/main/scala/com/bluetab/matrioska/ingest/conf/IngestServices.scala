package com.bluetab.matrioska.ingest.conf

import com.bluetab.matrioska.ingest.services.impl.NetcashAnalyticsServiceImpl
import com.bluetab.matrioska.ingest.services.TickHistoryService
import com.bluetab.matrioska.ingest.services.TickHistory1Service
import com.bluetab.matrioska.ingest.services.impl.InformacionalServiceImpl
import com.bluetab.matrioska.ingest.services.impl.EvaServiceImpl
import com.bluetab.matrioska.ingest.services.impl.TickHistory1ServiceImpl
import com.bluetab.matrioska.ingest.services.impl.PrerawMaintenanceServiceImpl
import com.bluetab.matrioska.ingest.services.StagingToPrerawService
import com.bluetab.matrioska.ingest.services.impl.DataIngestionServiceImpl
import com.bluetab.matrioska.ingest.services.DataIngestionService
import com.bluetab.matrioska.ingest.services.impl.MetadataServiceImpl
import com.bluetab.matrioska.ingest.services.NetcashAnalyticsService
import com.bluetab.matrioska.ingest.services.impl.PrerawToRawServiceImpl
import com.bluetab.matrioska.ingest.services.EvaService
import com.bluetab.matrioska.ingest.services.GsdsService
import com.bluetab.matrioska.ingest.services.MetadataService
import com.bluetab.matrioska.ingest.services.PrerawMaintenanceService
import com.bluetab.matrioska.ingest.services.InformacionalService
import com.bluetab.matrioska.ingest.services.PrerawToRawService
import com.bluetab.matrioska.ingest.services.impl.GMCalculatedDataServiceImpl
import com.bluetab.matrioska.ingest.services.impl.StagingToPrerawServiceImpl
import com.bluetab.matrioska.ingest.services.impl.GsdsServiceImpl
import com.bluetab.matrioska.ingest.services.CommonService
import com.bluetab.matrioska.ingest.services.impl.CommonServiceImpl
import com.bluetab.matrioska.ingest.services._
import com.bluetab.matrioska.ingest.services.impl._
import com.bluetab.matrioska.services.impl._



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
