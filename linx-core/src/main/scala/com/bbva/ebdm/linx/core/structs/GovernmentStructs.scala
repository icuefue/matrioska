package com.bbva.ebdm.linx.core.structs

import org.apache.spark.sql.types._

object GovernmentStructs {

  val auditRaw =
    StructType(
      StructField("fecha", StringType, true) ::
        StructField("des_uuid", StringType, true) ::
        StructField("des_malla", StringType, true) ::
        StructField("des_jobbd", StringType, true) ::
        StructField("caso_uso", StringType, true) ::
        StructField("proceso", StringType, true) ::
        StructField("tipo_ingesta", StringType, true) ::
        StructField("frecuencia", StringType, true) ::
        StructField("nombre_obj_origen", StringType, true) ::
        StructField("nro_reg_obj_origen", IntegerType, true) ::
        StructField("nombre_obj_destino", StringType, true) ::
        StructField("nro_reg_obj_destino", IntegerType, true) ::
        StructField("usuario", StringType, true) ::
        StructField("ejecutable", StringType, true) ::
        StructField("descripcion", StringType, true) ::
        StructField("fechaproceso", StringType, true) ::
        Nil)

  val auditMaster =
    StructType(
      StructField("fecha", StringType, true) ::
        StructField("des_uuid", StringType, true) ::
        StructField("des_malla", StringType, true) ::
        StructField("des_jobbd", StringType, true) ::
        StructField("caso_uso", StringType, true) ::
        StructField("proceso", StringType, true) ::
        StructField("tipo_carga", StringType, true) ::
        StructField("frecuencia", StringType, true) ::
        StructField("nombre_obj_origen", StringType, true) ::
        StructField("nro_reg_obj_origen", DoubleType, true) ::
        StructField("nombre_obj_destino", StringType, true) ::
        StructField("nro_reg_obj_destino", DoubleType, true) ::
        StructField("usuario", StringType, true) ::
        StructField("ejecutable", StringType, true) ::
        StructField("descripcion", StringType, true) ::
        Nil)

  val logDetail =
    StructType(
      StructField("fec_hms_timestamp_log", StringType, true) ::
        StructField("des_uuid", StringType, true) ::
        StructField("des_estado_proceso", StringType, true) ::
        StructField("des_malla", StringType, true) ::
        StructField("des_jobbd", StringType, true) ::
        StructField("des_scriptbd", StringType, true) ::
        StructField("des_capabd", StringType, true) ::
        StructField("fec_fecha_planificacion", StringType, true) ::
        StructField("des_level", StringType, true) ::
        StructField("des_tag", StringType, true) ::
        StructField("des_traza", StringType, true) ::
        Nil)

  val logExecutions =
    StructType(
      StructField("fec_hms_timestamp_log", StringType, true) ::
        StructField("des_uuid", StringType, true) ::
        StructField("des_estado_proceso", StringType, true) ::
        StructField("des_malla", StringType, true) ::
        StructField("des_jobbd", StringType, true) ::
        StructField("des_scriptbd", StringType, true) ::
        StructField("des_capabd", StringType, true) ::
        StructField("fec_fecha_planificacion", StringType, true) ::
        Nil)
}


 