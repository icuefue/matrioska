package com.bbva.ebdm.linx.ingest.apps

import com.bbva.ebdm.linx.core.LinxApp
import com.bbva.ebdm.linx.ingest.conf.IngestServices
import com.bbva.ebdm.linx.ingest.constants.IngestDfsConstants
import com.bbva.ebdm.linx.core.conf.CoreRepositories
import org.joda.time.DateTime
import java.util.Date
import java.text.SimpleDateFormat
import com.bbva.ebdm.linx.core.conf.CoreContext

class GsdsLoadMasterApp extends LinxApp {

  override def run(args: Seq[String]) {

    if (args.length == 1) {
      args(0) match {
        case "001" =>
          IngestServices.gsdsService.createMasterViewOrdersEurexIon
          IngestServices.gsdsService.deleteGsdsFlag(IngestDfsConstants.GsdsFlag001)
        case "002" =>
          IngestServices.gsdsService.createMasterViewRfqIon
          IngestServices.gsdsService.deleteGsdsFlag(IngestDfsConstants.GsdsFlag002)
        case "003" =>
          IngestServices.gsdsService.createMasterViewRfqRet
          IngestServices.gsdsService.deleteGsdsFlag(IngestDfsConstants.GsdsFlag003)
        case "004" =>
          IngestServices.gsdsService.createMasterViewTradesEurexIon
          IngestServices.gsdsService.deleteGsdsFlag(IngestDfsConstants.GsdsFlag004)
        case "006" =>
          val flagFileContent = IngestServices.gsdsService.getGsdsFlagPipeline(IngestDfsConstants.GsdsFlag006)
          flagFileContent match {
            case Some(oDate) =>
              CoreContext.logger.info("oDate: " + oDate.toString())
              val date = new SimpleDateFormat("yyyyMMdd").parse(oDate)
              IngestServices.gsdsService.deletePriBonusPartition(date)
              IngestServices.gsdsService.loadTablePriBonus(date)
              IngestServices.gsdsService.auditTablePriBonus(date)
              IngestServices.gsdsService.deleteGsdsFlag(IngestDfsConstants.GsdsFlag006)
            case None =>
          }

      }
    }

  }

}
