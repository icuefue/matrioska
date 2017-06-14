package com.bbva.ebdm.linx.ingest.conf

import com.bbva.ebdm.linx.ingest.services.impl.NetcashAnalyticsServiceImpl
import com.bbva.ebdm.linx.ingest.services.TickHistoryService
import com.bbva.ebdm.linx.ingest.services.TickHistory1Service
import com.bbva.ebdm.linx.ingest.services.impl.InformacionalServiceImpl
import com.bbva.ebdm.linx.ingest.services.impl.EvaServiceImpl
import com.bbva.ebdm.linx.ingest.services.impl.TickHistoryServiceImpl
import com.bbva.ebdm.linx.ingest.services.impl.TickHistory1ServiceImpl
import com.bbva.ebdm.linx.ingest.services.impl.PrerawMaintenanceServiceImpl
import com.bbva.ebdm.linx.ingest.services.StagingToPrerawService
import com.bbva.ebdm.linx.ingest.services.impl.DataIngestionServiceImpl
import com.bbva.ebdm.linx.ingest.services.DataIngestionService
import com.bbva.ebdm.linx.ingest.services.impl.MetadataServiceImpl
import com.bbva.ebdm.linx.ingest.services.NetcashAnalyticsService
import com.bbva.ebdm.linx.ingest.services.impl.PrerawToRawServiceImpl
import com.bbva.ebdm.linx.ingest.services.EvaService
import com.bbva.ebdm.linx.ingest.services.GsdsService
import com.bbva.ebdm.linx.ingest.services.MetadataService
import com.bbva.ebdm.linx.ingest.services.PrerawMaintenanceService
import com.bbva.ebdm.linx.ingest.services.InformacionalService
import com.bbva.ebdm.linx.ingest.services.PrerawToRawService
import com.bbva.ebdm.linx.ingest.services.GMCalculatedDataService
import com.bbva.ebdm.linx.ingest.services.impl.GMCalculatedDataServiceImpl
import com.bbva.ebdm.linx.ingest.services.impl.StagingToPrerawServiceImpl
import com.bbva.ebdm.linx.ingest.services.impl.GsdsServiceImpl
import com.bbva.ebdm.linx.ingest.services.CommonService
import com.bbva.ebdm.linx.ingest.services.impl.CommonServiceImpl



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
