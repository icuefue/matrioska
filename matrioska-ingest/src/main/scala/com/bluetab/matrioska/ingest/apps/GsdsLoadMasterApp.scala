package com.bluetab.matrioska.ingest.apps

import org.joda.time.DateTime
import java.util.Date
import java.text.SimpleDateFormat

import com.bluetab.matrioska.core.conf.CoreContext
import com.bluetab.matrioska.core.LinxApp
import com.bluetab.matrioska.core.conf.CoreRepositories
import com.bluetab.matrioska.ingest.conf.IngestServices
import com.bluetab.matrioska.ingest.constants.IngestDfsConstants

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
