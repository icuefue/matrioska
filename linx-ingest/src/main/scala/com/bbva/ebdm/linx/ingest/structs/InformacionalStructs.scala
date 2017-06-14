package com.bbva.ebdm.linx.ingest.structs

import org.apache.spark.sql.types.{StringType, StructField, StructType}

object InformacionalStructs {

  val timeInfo =
    StructType(
      StructField("fecFechaS", StringType, true) ::
        StructField("codAnioS", StringType, true) ::
        StructField("codMesS", StringType, true) ::
        StructField("codSemeS", StringType, true) ::
        StructField("codTrimS", StringType, true) ::
        StructField("codSemaS", StringType, true) ::
        StructField("codFechaNum", StringType, true) ::
        StructField("qnuDiaanio", StringType, true) ::
        StructField("fecDiaant", StringType, true) ::
        StructField("fecAniddant", StringType, true) ::
        StructField("fecMesddant", StringType, true) ::
        StructField("fecTriddant", StringType, true) ::
        StructField("fecSemddant", StringType, true) ::
        StructField("desDiaSem", StringType, true) ::
        StructField("desDenSem", StringType, true) ::
        StructField("desDSCor", StringType, true) ::
        StructField("desDSCen", StringType, true) ::
        StructField("fecSa", StringType, true) ::
        StructField("fecDaMa", StringType, true) ::
        StructField("fecDaMaAa", StringType, true) ::
        StructField("fecDaAa", StringType, true) ::
        StructField("fecUdMa", StringType, true) ::
        StructField("fecUdAa", StringType, true) ::
        StructField("codRegid", StringType, true) ::
        StructField("codAudit", StringType, true) ::
        StructField("audUsuario", StringType, true) ::
        StructField("audTim", StringType, true) :: Nil)
}
