package com.bluetab.matrioska.ingest.apps

import com.bluetab.matrioska.core.LinxAppArgs
import java.text.SimpleDateFormat
import java.util.Date

import EvaUseCases.{Engloba, Eva, Heffa, Pipeline, TablasGenerales}
import com.bluetab.matrioska.core.{LinxApp, LinxAppArgs}
import com.bluetab.matrioska.ingest.conf.IngestServices

/**
  * App de carga en el master del proyecto EVA
  *
  *3 casos de uso:
  * 1) EVA
  * 2) PIPELINE
  * 3) ENGLOBA
  * 4) HEFFA
  * 5) TABLASGENERALES
  */
class EvaLoadMasterApp extends LinxApp {

  override def run(args: Seq[String]) {

    if (args.nonEmpty) {
      args.head match {
        case "EVA" => eva()
        case "PIPELINE" => pipeline()
        case "ENGLOBA" => engloba()
        case "HEFFA" => heffa()
        case "TABLASGENERALES" => tablasGenerales()
      }
    }

  }

  /**
    * Caso de uso EVA
    */
  private def eva() = {
    // Borrado de las tablas de Eva en el master (md_eva.eva, md_tablasgenerales.paisdivisasede, md_eva.producto,...)
    IngestServices.evaService.deleteTablesAndPartitions(Eva, getDate)

    // Carga de las tablas de Eva del master con las tablas de Eva del raw
    IngestServices.evaService.loadMaster(Eva, getDate)

    // Auditoría de lo cargado en master
    IngestServices.evaService.auditTables(Eva, getDate)

    // Borrado de los flags generados
    IngestServices.evaService.deleteFlags(Eva)
  }

  /**
    * Caso de uso PIPELINE
    */
  private def pipeline() = {
    // Borrado de las tablas de Pipeline en el master (No aplica)
    IngestServices.evaService.deleteTablesAndPartitions(Pipeline, getDate)

    // Carga de las vistas de Pipeline del master con las tablas de Pipeline del raw
    IngestServices.evaService.loadMaster(Pipeline, getDate)

    // Auditoría de lo cargado en master
    IngestServices.evaService.auditTables(Pipeline, getDate)

    // Borrado de los flags generados
    IngestServices.evaService.deleteFlags(Pipeline)
  }

  /**
    * Caso de uso ENGLOBA
    */
  private def engloba() = {
    // Borrado de las tablas de Engloba en el master (md_eva.engloba)
    IngestServices.evaService.deleteTablesAndPartitions(Engloba, getDate)

    // Carga de la tabla de Engloba del master con las tablas de engloba del raw
    IngestServices.evaService.loadMaster(Engloba, getDate)

    // Auditoría de lo cargado en master
    IngestServices.evaService.auditTables(Engloba, getDate)

    // Borrado de los flags generados
    IngestServices.evaService.deleteFlags(Engloba)
  }

  /**
    * Caso de uso HEFFA
    */
  private def heffa() = {
    // La carga en el master de eva no es más que una vista estática que actúa sobre la tabla rd_informacional.tendsrfq
    IngestServices.evaService.deleteTablesAndPartitions(Heffa, getDate)
    IngestServices.evaService.loadMaster(Heffa, getDate)
    IngestServices.evaService.auditTables(Heffa, getDate)
    IngestServices.evaService.deleteFlags(Heffa)
  }

  /**
    * Caso de uso TABLASGENERALES
    */
  private def tablasGenerales() = {
    // Borrado de las tablas de Tablas Generales en el master (md_tablasgenerales.clientes, md_tablasgenerales.clientes_int,...)
    IngestServices.evaService.deleteTablesAndPartitions(TablasGenerales, getDate)

    // Carga de las tablas de Tablas Generales con las tablas del raw
    IngestServices.evaService.loadMaster(TablasGenerales, getDate)

    // Auditoría de lo cargado en master
    IngestServices.evaService.auditTables(TablasGenerales, getDate)

    // Borrado de los flags generados
    IngestServices.evaService.deleteFlags(TablasGenerales)

  }

  private def getDate: Date = {
    new SimpleDateFormat("yyyyMMdd").parse(LinxAppArgs.planDate)
  }
}

object EvaUseCases {
  sealed trait EvaUseCase
  case object Eva extends EvaUseCase
  case object Pipeline extends EvaUseCase
  case object Engloba extends EvaUseCase
  case object Heffa extends EvaUseCase
  case object TablasGenerales extends EvaUseCase
}
