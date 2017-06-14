package com.bbva.ebdm.linx.core.utils

import org.apache.spark.sql.Row

object GovernmentUtils {
  def formatDetailsLogs(line: String): Row = {
    val words = parselogs(line)
    if (words.size == 12){
      Row(words(1),
        words(2),
        words(3),
        words(4),
        words(5),
        words(6),
        words(7),
        words(8),
        words(9),
        words(10),
        words(11))
    }
    else
      Row("")
  }

  def formatExecutionLogs(line: String): Row = {
    val words = parselogs(line)
    if (words.size == 12){
      Row(words(1),
        words(2),
        words(3),
        words(4),
        words(5),
        words(6),
        words(7),
        words(8))
    }
    else
      Row("")
  }

  def formatAuditRaw(line: String): Row = {
    val words = parselogs(line)
    if (words.size == 12) {
      val auditVals = words(11).split("\\|")
      Row(words(1), //fecha
        words(2), //des_uuid
        words(4), //des_malla
        words(5), //des_jobbd
        words(6), //caso_uso
        words(6), //proceso
        words(7), //tipo_ingesta
        "", //frecuencia
        auditVals(0), //nombre_obj_origen
        auditVals(1).toInt, //nro_reg_obj_origen
        auditVals(2), //nombre_obj_destino
        auditVals(3).toInt, //nro_reg_obj_origen
        "", //usuario
        words(6), //ejecutable
        "Auditoria RAW", //descripcion
        words(8) //fec_fecha_planificacion
        )
    } else
      Row("")
  }
  
  def parselogs(line:String):Array[String] = {
    line.replaceAll("[\n\r]"," ").split("\t")
  }
}