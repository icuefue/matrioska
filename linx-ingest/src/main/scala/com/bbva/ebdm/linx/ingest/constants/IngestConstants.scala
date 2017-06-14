package com.bbva.ebdm.linx.ingest.constants

object IngestConstants {

  val SufixProcesando = "_INPROGRESS"
  val SufixProcesado = "_PROCESADO"
  val RawSchemaRisks = "rd_gm_calculated_data"
  val MasterSchemaRisks = "md_gm_calculated_data"

  val HqlGsdsStocks = "/hql/gsds/stocks.hql"
  val HqlGsdsEventsMaxDate = "/hql/gsds/events_max_date.hql"
  val HqlGsdsEndOfDays = "/hql/gsds/endofdays.hql"
  val HqlGsdsDividendsMaxDate = "/hql/gsds/dividends_max_date.hql"
  val HqlGsdsRfqionMaxDate = "/hql/gsds/rfqion_max_date.hql"

  val HqlGsds001CreateView = "/hql/gsds/gsds001_master_create_view.hql"
  val HqlGsds002CreateView = "/hql/gsds/gsds002_master_create_view.hql"
  val HqlGsds003CreateView = "/hql/gsds/gsds003_master_create_view.hql"
  val HqlGsds004CreateView = "/hql/gsds/gsds004_master_create_view.hql"

  val HqlGsds001MaxFecConc = "/hql/gsds/gsds001_max_fec_conc.hql"
  val HqlGsds002MaxFecConc = "/hql/gsds/gsds002_max_fec_conc.hql"
  val HqlGsds003MaxFecConc = "/hql/gsds/gsds003_max_fec_conc.hql"
  val HqlGsds004MaxFecConc = "/hql/gsds/gsds004_max_fec_conc.hql"

  val HqlGsds006DropPartitionPribonus = "/hql/gsds/gsds006_drop_partition_pribonus.hql"
  val HqlGsds006LoadTablePribonus = "/hql/gsds/gsds006_load_table_pribonus.hql"
  val HqlGsds006AuditTablePribonus = "/hql/gsds/gsds006_audit_table_pribonus.hql"
  val HqlGsds006CreateFunctionRowSequence = "/hql/gsds/gsds006_create_function_row_sequence.hql"

  // Eva
  val HqlEvaLoadPartitionEva = "/hql/eva/eva/load_partition_eva.hql"
  val HqlEvaLoadPartitionGrupoFicticio = "/hql/eva/eva/load_partition_grupo_ficticio.hql"
  val HqlEvaLoadPartitionGrupo = "/hql/eva/eva/load_partition_grupo.hql"
  val HqlEvaLoadPartitionPaisDivisaSede = "/hql/eva/eva/load_partition_pais_divisa_sede.hql"
  val HqlEvaLoadPartitionParametro = "/hql/eva/eva/load_partition_parametro.hql"
  val HqlEvaLoadPartitionProducto = "/hql/eva/eva/load_partition_producto.hql"
  val HqlEvaLoadPartitionTiposDeCambio = "/hql/eva/eva/load_partition_tipos_de_cambio.hql"
  val HqlEvaAuditEva = "/hql/eva/eva/audit_eva.hql"
  val HqlEvaAuditEngloba = "/hql/eva/engloba/audit_engloba.hql"
  val HqlEvaAuditGrupoFicticio = "/hql/eva/eva/audit_grupo_ficticio.hql"
  val HqlEvaAuditGrupo = "/hql/eva/eva/audit_grupo.hql"
  val HqlEvaAuditPaisDivisaSede = "/hql/eva/eva/audit_pais_divisa_sede.hql"
  val HqlEvaAuditParametro = "/hql/eva/eva/audit_parametro.hql"
  val HqlEvaAuditProducto = "/hql/eva/eva/audit_producto.hql"
  val HqlEvaAuditTiposDeCambio = "/hql/eva/eva/audit_tipos_de_cambio.hql"

  // Pipeline
  val HqlEvaPipelineAlterViewTenspdov = "/hql/eva/pipeline/alterview_tenspdov_pipeline.hql"
  val HqlEvaPipelineAlterViewTenspdpi = "/hql/eva/pipeline/alterview_tenspdpi_pipeline.hql"
  val HqlEvaPipelineAuditTenspdov = "/hql/eva/pipeline/audit_tenspdov_pipeline.hql"
  val HqlEvaPipelineAuditTenspdpi = "/hql/eva/pipeline/audit_tenspdpi_pipeline.hql"

  // Engloba
  val HqlEvaEnglobaLoadPartition = "/hql/eva/engloba/load_partition_engloba.hql"

  // Tablas generales
  val HqlEvaTtggLoadPartitionClientes = "/hql/eva/tablasgenerales/load_partition_clientes.hql"
  val HqlEvaTtggLoadPartitionClientesInt = "/hql/eva/tablasgenerales/load_partition_clientes_int.hql"
  val HqlEvaTtggLoadPartitionDivisas = "/hql/eva/tablasgenerales/load_partition_divisas.hql"
  val HqlEvaTtggLoadPartitionTiposCambio = "/hql/eva/tablasgenerales/load_partition_tipos_cambio.hql"
  val HqlEvaTtggLoadPartitionOficinas = "/hql/eva/tablasgenerales/load_partition_oficinas.hql"
  val HqlEvaTtggAuditClientes =  "/hql/eva/tablasgenerales/audit_clientes.hql"
  val HqlEvaTtggAuditClientesInt = "/hql/eva/tablasgenerales/audit_clientes_int.hql"
  val HqlEvaTtggAuditDivisas = "/hql/eva/tablasgenerales/audit_divisas.hql"
  val HqlEvaTtggAuditTiposCambio = "/hql/eva/tablasgenerales/audit_tipos_cambio.hql"
  val HqlEvaTtggAuditOficinas = "/hql/eva/tablasgenerales/audit_oficinas.hql"
  val HqlEvaTtggAlterViewAgrupacionesCrmPTfm = "/hql/eva/tablasgenerales/alterview_agrupaciones_crm_p_tfm.hql"
  val HqlEvaTtggAlterViewAgrupaciones = "/hql/eva/tablasgenerales/alterview_agrupaciones.hql"
  val HqlEvaTtggAlterViewParticipeGrupoCrm = "/hql/eva/tablasgenerales/alterview_participe_grupo_crm.hql"
  val HqlEvaTtggAlterViewParticipeGrupoTfm = "/hql/eva/tablasgenerales/alterview_participe_grupo_tfm.hql"
  val HqlEvaTtggAlterViewParticipes = "/hql/eva/tablasgenerales/alterview_participes.hql"
  val HqlEvaTtggAlterViewRelaAgrupaciones = "/hql/eva/tablasgenerales/alterview_rela_agrupaciones.hql"
  val HqlEvaTtggAlterViewTipoAgrupacion = "/hql/eva/tablasgenerales/alterview_tipo_agrupacion.hql"

  val HqlMetadataGetSources = "/hql/metadata/get_sources.hql"
  val HqlMetadataGetStagingPaths = "/hql/metadata/get_staging_paths.hql"
  val HqlMetadataGetUseCaseItems = "/hql/metadata/get_usecase_items.hql"

  // Netcash
  val HqlNetcashAnalyticsAlterViewContratos = "/hql/netcash/netcash_sstt01_contratos_create_view.hql"
  val HqlNetcashAnalyticsAlterViewServiciosContratados = "/hql/netcash/netcash_sstt02_servicios_contratados_create_view.hql"
  val HqlNetcashAnalyticsAlterViewNormalizador = "/hql/netcash/netcash_sstt03_normalizador_create_view.hql"
  val HqlNetcashAnalyticsAlterViewCatalogoCanales = "/hql/netcash/netcash_sstt04_catalogo_canales_create_view.hql"
  val HqlNetcashAnalyticsAlterViewCatalogoServicios = "/hql/netcash/netcash_sstt05_catalogo_servicios_create_view.hql"
  val HqlNetcashAnalyticsAlterViewCatalogoSituacionFicheros = "/hql/netcash/netcash_sstt07_catalogo_situacion_ficheros_create_view.hql"
  val HqlNetcashAnalyticsAlterViewCatalogoFormatoFicheros = "/hql/netcash/netcash_sstt08_catalogo_formato_ficheros_create_view.hql"
  val HqlNetcashAnalyticsAlterViewUsuarios = "/hql/netcash/netcash_nc01_usuarios_create_view.hql"
  val HqlNetcashAnalyticsAlterViewPerfilCliente = "/hql/netcash/netcash_nc03_perfil_cliente_create_view.hql"

  val HqlPrerawToRawGetSourcenames = "/hql/prerawtoraw/get_sourcenames.hql"
  val HqlPrerawToRawSelectForReprocess = "/hql/prerawtoraw/select_reprocess.hql"

  val HqlInformacionalSelectAllLastPartition = "/hql/informacional/select_all_last_partition.hql"
}
