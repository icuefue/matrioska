package com.bluetab.matrioska.ingest.apps

import java.text.DateFormat
import java.text.SimpleDateFormat
import java.util.Date

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SaveMode
import com.bluetab.matrioska.core.LinxAppArgs
import com.bluetab.matrioska.core.conf.CoreContext
import com.bluetab.matrioska.core.conf.CoreServices
import com.bluetab.matrioska.core.conf.CoreRepositories
import com.bluetab.matrioska.core.exceptions.FatalException
import com.bluetab.matrioska.core.{LinxApp, LinxAppArgs}
import com.bluetab.matrioska.ingest.conf.IngestServices

class TickHistoryApp extends LinxApp {

  def tableList = ("t_endofday", "t_endofday") :: 
                  ("t_marketdepth", "t_marketdepth") ::
                  ("t_intraday", "t_intraday") ::
                  ("t_timeandsales", "t_timeandsales") :: Nil

  override def run(args: Seq[String]) {

    val formatter: DateFormat = new SimpleDateFormat("yyyyMMdd");
    val planDate = formatter.parse(LinxAppArgs.planDate);

    tableList.map( table => {
      val inputDF = IngestServices.tickHistory1Service.loadHiveRawTablePartitionInDate(
            "rd_gsds",
            table._1,
            planDate)

      IngestServices.tickHistory1Service.replaceMasterPartition(
            inputDF,
            "md_gsds",
            table._2 + "_new")

    })
  }

}
