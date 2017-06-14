package com.bluetab.matrioska.ingest.constants

object IngestDfsConstants {

  val EnglobaPipelineFlag = "/cfg/informacional/flag/informacional_engloba_raw.flag"
  val GovernmentLogExecutions = "/raw/ebdmau/t_log_executions/spark"
  val GovernmentLogDetails = "/raw/ebdmau/t_log_details/spark"
  val GovernmentAuditRaw = "/raw/ebdmau/t_audit_raw2/spark"
  val GsdsAdjustmentFactor = "/master/gsds/t_factor_adj/resultado.txt"
  val GsdsEvents = "/raw/typhoon/t_events"
  val GsdsDividends = "/raw/typhoon/t_dividends"
  val GsdsEndofdays = "/raw/typhoon/t_endofday"
  val InformacionalTimeInfo = "/raw/informacional/tablasgenerales/tenydfec"
  val AuditoryPrueba = "/raw/ebdmgv/prueba_calidad"
  val GsdsRfqion = "/raw/intranet/gsds/rfq_ion"
  val PruebaSergio = "/user/xe58268/data_ingestion/app"
  val PreRawOriginPath = "/raw/preraw"
  val PrerawToLoad = "/raw/preraw/to_load"
  val PrerawInProgress = "/raw/preraw/in_progress"
  val PrerawFailures = "/raw/preraw/failures"
  val PrerawSuccessful = "/raw/preraw/successful"

  val GsdsFlag001 = "/cfg/gsds/flag/GSDS001_RAW.flg"
  val GsdsFlag002 = "/cfg/gsds/flag/GSDS002_RAW.flg"
  val GsdsFlag003 = "/cfg/gsds/flag/GSDS003_RAW.flg"
  val GsdsFlag004 = "/cfg/gsds/flag/GSDS004_RAW.flg"
  val GsdsFlag006 = "/cfg/gsds/flag/raw_gsds6.flg"

  val FlagsRaw = "/cfg/flags/raw"

  val GsdsTPriBonusPath = "/master/gsds/t_pri_bonus"

  // master de eva (eva)
  val EvaEvaTablePath = "/master/eva/eva"
  val EvaGrupoFicticioTablePath = "/master/eva/grupoficticio"
  val EvaParametroTablePath = "/master/eva/parametro"
  val EvaProductoTablePath = "/master/eva/producto"
  val TtggGrupoTablePath = "/master/tablasgenerales/grupo"
  val TtggTiposDeCambioTablePath = "/master/tablasgenerales/tiposdecambio"
  val TtggPaisDivisaSedeTablePath = "/master/tablasgenerales/paisdivisasede"

  // master de engloba (eva)
  val EvaEnglobaTablePath = "/master/eva/engloba"

  // master de tablas generales (tablas generales)
  val TtggClientesTablePath = "/master/tablasgenerales/clientes"
  val TtggClientesIntTablePath = "/master/tablasgenerales/clientes_int"
  val TtggDivisasTablePath = "/master/tablasgenerales/divisas"
  val TtggTiposCambioTablePath = "/master/tablasgenerales/tipos_cambio"
  val TtggOficinasTablePath = "/master/tablasgenerales/oficinas"
}
