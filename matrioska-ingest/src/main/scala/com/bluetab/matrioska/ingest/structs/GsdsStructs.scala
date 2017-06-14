package com.bluetab.matrioska.ingest.structs

import org.apache.spark.sql.types._

object GsdsStructs {

  val endofdays =
    StructType(
      StructField("ric", StringType, true) ::
        StructField("tick_date", StringType, true) ::
        StructField("tick_time", StringType, true) ::
        StructField("rickType", StringType, true) ::
        StructField("open", DoubleType, true) ::
        StructField("high", DoubleType, true) ::
        StructField("low", DoubleType, true) ::
        StructField("last", DoubleType, true) ::
        StructField("volume", IntegerType, true) ::
        StructField("settle", DoubleType, true) ::
        StructField("closeBid", DoubleType, true) ::
        StructField("closeAsk", DoubleType, true) ::
        StructField("volumeMultiplier", DoubleType, true) :: Nil)

  val dividend =
    StructType(
      StructField("ric", StringType, true) ::
        StructField("ricType", StringType, true) ::
        StructField("searchType", StringType, true) ::
        StructField("dseFileDate", StringType, true) ::
        StructField("keyDate1", StringType, true) ::
        StructField("keyDate2", StringType, true) ::
        StructField("keyDate3", StringType, true) ::
        StructField("currentSystemDate", StringType, true) ::
        StructField("dividendAmountRate", StringType, true) ::
        StructField("dividendCurrency", StringType, true) ::
        StructField("dividendMarketLevelId", IntegerType, true) ::
        StructField("dividendPayDate", StringType, true) :: Nil)

  val event =
    StructType(
      StructField("ric", StringType, true) ::
        StructField("ricType", StringType, true) ::
        StructField("searchType", StringType, true) ::
        StructField("dseFileDate", StringType, true) ::
        StructField("keyDate1", StringType, true) ::
        StructField("keyDate2", StringType, true) ::
        StructField("keyDate3", StringType, true) ::
        StructField("currentSystemDate", StringType, true) ::
        StructField("adjustmentFactor", StringType, true) ::
        StructField("capitalChangeEventDescription", StringType, true) ::
        StructField("effectiveDate", StringType, true) :: Nil)

  val rfqion =
    StructType(
      StructField("id", StringType, true) ::
      StructField("marketid", StringType, true) ::
      StructField("actionstr", StringType, true) ::
      StructField("anctstier", IntegerType, true) ::
        StructField("aqquoteowner", StringType, true) ::
        StructField("code", StringType, true) ::
        StructField("codetypestr", StringType, true) ::
        StructField("currencystr", StringType, true) ::
        StructField("custfirm", StringType, true) ::
        StructField("custusername", StringType, true) ::
        StructField("dealertrader", StringType, true) ::
        StructField("dealertradername", StringType, true) ::
        StructField("descripcion", StringType, true) ::
        StructField("maturitydate", StringType, true) ::
        StructField("rfqcoverprice", DoubleType, true) ::
        StructField("rfqcreatedate", StringType, true) ::
        StructField("rfqnlegs", IntegerType, true) ::
        StructField("rfqordertypestr", StringType, true) ::
        StructField("rfqprice", DoubleType, true) ::
        StructField("rfqqty", DoubleType, true) ::
        StructField("rfqtypestr", StringType, true) ::
        StructField("rfqverbstr", StringType, true) ::
        StructField("rfqyield", DoubleType, true) ::
        StructField("salesperson", StringType, true) ::
        StructField("salespersonid", StringType, true) ::
        StructField("salespersonroom", StringType, true) ::
        StructField("statusstr", StringType, true) ::
        StructField("tradeid", StringType, true) ::
        StructField("ask0", DoubleType, true) ::
        StructField("bid0", DoubleType, true) ::
        StructField("cpaccountid", StringType, true) ::
        StructField("cpdescription", StringType, true) ::
        StructField("country", StringType, true) ::
        StructField("countryisocode", StringType, true) ::
        StructField("customstring5", StringType, true) ::
        StructField("ionbook", StringType, true) ::
        StructField("sensitivity", DoubleType, true) ::

        Nil)

}


 