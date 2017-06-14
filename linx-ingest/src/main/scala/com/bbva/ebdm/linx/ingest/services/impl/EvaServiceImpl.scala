package com.bbva.ebdm.linx.ingest.services.impl

import com.bbva.ebdm.linx.ingest.services.EvaService
import com.bbva.ebdm.linx.core.conf.{CoreAppInfo, CoreRepositories}
import com.bbva.ebdm.linx.ingest.constants.IngestConstants
import java.util.{Calendar, Date}
import java.text.SimpleDateFormat

import com.bbva.ebdm.linx.core.LinxAppArgs
import com.bbva.ebdm.linx.core.constants.CoreConstants
import com.bbva.ebdm.linx.ingest.apps.EvaUseCases.{Engloba, Eva, EvaUseCase, Heffa, Pipeline, TablasGenerales}
import com.bbva.ebdm.linx.ingest.conf.IngestServices
import com.bbva.ebdm.linx.ingest.constants.IngestDfsConstants

object EvaServiceConstants {
  val format1 = new SimpleDateFormat("yyyyMMdd")
  val format2 = new SimpleDateFormat("dd/MM/yyyy HH:mm:ss:SSS")

  // Esquema en raw del proyecto EVA
  val RdInformacional = "rd_informacional"

  // Esquemas en master del proyecto EVA
  val MdEva = "md_eva"
  val MdTablasGenerales = "md_tablasgenerales"

  // Tablas en el master del proyecto EVA (md_eva)
  val Engloba = "engloba"
  val Eva = "eva"
  val Grupoficticio = "grupoficticio"
  val Parametro = "parametro"
  val Producto = "producto"

  // Vistas en el master del proyecto EVA (md_eva)
  val Vtenspdov = "v_tenspdov"
  val Vtenspdpi = "v_tenspdpi"

  // Tablas en el master del proyecto EVA (md_tablasgenerales)
  val Grupo = "grupo"
  val PaisDivisaSede = "paisdivisasede"
  val TiposDeCambio = "tiposdecambio"
  val Clientes = "clientes"
  val ClientesInt = "clientes_int"
  val Divisas = "divisas"
  val TiposCambio = "tipos_cambio"
  val Oficinas = "oficinas"

  // Esquema de auditoria
  val RdEbdmau = "rd_ebdmau"

  val TAuditMaster = "t_audit_master"

  // Tablas de Eva
  val Tendvehs ="tendvehs"
  val Tendvtcb ="tendvtcb"
  val Tenwfduc ="tenwfduc"
  val Tentgges ="tentgges"
  val Tenydgup ="tenydgup"
  val Tenydytp ="tenydytp"
  val Tenydier ="tenydier"
  val Tenydpai ="tenydpai"
  val Tenydyps ="tenydyps"
  val Tenydyss ="tenydyss"
  val Tenydxed ="tenydxed"
  val Tenydcrs ="tenydcrs"
  val Tenydiur ="tenydiur"
  val Tenwfgfi ="tenwfgfi"
  val Tenwfkec ="tenwfkec"
  val Tenwfpec ="tenwfpec"
  val Tenwfpco ="tenwfpco"
  val Tenbtctr ="tenbtctr"
  val Tentgtcm ="tentgtcm"
  val Tentgytr = "tentgytr"
  val Tentgdiv = "tentgdiv"
  val Tenydmgl = "tenydmgl"
  val Tenydmrg = "tenydmrg"
  val Tenydfec = "tenydfec"

  // Tablas de Pipeline
  val Tenspdov = "tenspdov"
  val Tenspdpi = "tenspdpi"

  // Tablas de Engloba
  val Tendgcel = "tendgcel"
  val Tendgcir = "tendgcir"
  val Tendgehr = "tendgehr"
  val Tendggur = "tendggur"
  val Tendgvdp = "tendgvdp"
  val Tendgwbp = "tendgwbp"
  val Tendgwje = "tendgwje"
  val Tendgwlr = "tendgwlr"
  val Tendgwne = "tendgwne"
  val Tendgwpa = "tendgwpa"
  val Tendgwqr = "tendgwqr"
  val Vendgeh1 = "vendgeh1"
  val Vendgwn1 = "vendgwn1"
  val Vendgwp1 = "vendgwp1"

  // Tablas de Tablas Generales
  val Tenyddcb = "tenyddcb"
  val Tenydgvc = "tenydgvc"
  val Tentgdvs = "tentgdvs"
  val Ventgtcm1 = "ventgtcm1"
  val Tenorgru = "tenorgru"
  val Tenjcgup = "tenjcgup"
  val Tenjcrkg = "tenjcrkg"
  val Tenjctag = "tenjctag"
  val Tenjcktl = "tenjcktl"
  val Tenjcrhg = "tenjcrhg"
  val Tenjctdf = "tenjctdf"
  val Tenjcrct = "tenjcrct"

  // Tablas de Heffa
  val Tendsrfq = "tendsrfq"

}

class EvaServiceImpl extends EvaService {

  override def deleteTablesAndPartitions(evaUseCase: EvaUseCase, date: Date): Unit = {
    evaUseCase match {
      case Eva =>

        CoreRepositories.hiveRepository.sql(CoreConstants.HqlCommonTableDropPartition, EvaServiceConstants.MdEva,
          EvaServiceConstants.Eva, s"year=${getYearFromDate(date)}, month=${getMonthFromDate(date)}, day=${getDayOfMonthFromDate(date)}")
        CoreRepositories.hiveRepository.sql(CoreConstants.HqlCommonTableDropPartition, EvaServiceConstants.MdEva,
          EvaServiceConstants.Grupoficticio, s"year=${getYearFromDate(date)}, month=${getMonthFromDate(date)}, day=${getDayOfMonthFromDate(date)}")
        CoreRepositories.hiveRepository.sql(CoreConstants.HqlCommonTableDropPartition, EvaServiceConstants.MdTablasGenerales,
          EvaServiceConstants.Grupo, s"year=${getYearFromDate(date)}, month=${getMonthFromDate(date)}, day=${getDayOfMonthFromDate(date)}")
        CoreRepositories.hiveRepository.sql(CoreConstants.HqlCommonTableDropPartition, EvaServiceConstants.MdTablasGenerales,
          EvaServiceConstants.PaisDivisaSede, s"year=${getYearFromDate(date)}, month=${getMonthFromDate(date)}, day=${getDayOfMonthFromDate(date)}")
        CoreRepositories.hiveRepository.sql(CoreConstants.HqlCommonTableDropPartition, EvaServiceConstants.MdEva,
          EvaServiceConstants.Parametro, s"year=${getYearFromDate(date)}, month=${getMonthFromDate(date)}, day=${getDayOfMonthFromDate(date)}")
        CoreRepositories.hiveRepository.sql(CoreConstants.HqlCommonTableDropPartition, EvaServiceConstants.MdEva,
          EvaServiceConstants.Producto, s"year=${getYearFromDate(date)}, month=${getMonthFromDate(date)}, day=${getDayOfMonthFromDate(date)}")
        CoreRepositories.hiveRepository.sql(CoreConstants.HqlCommonTableDropPartition, EvaServiceConstants.MdTablasGenerales,
          EvaServiceConstants.TiposDeCambio, s"year=${getYearFromDate(date)}, month=${getMonthFromDate(date)}, day=${getDayOfMonthFromDate(date)}")

        this.deletePartitionContent(IngestDfsConstants.EvaEvaTablePath, date)
        this.deletePartitionContent(IngestDfsConstants.EvaGrupoFicticioTablePath, date)
        this.deletePartitionContent(IngestDfsConstants.EvaParametroTablePath, date)
        this.deletePartitionContent(IngestDfsConstants.EvaProductoTablePath, date)
        this.deletePartitionContent(IngestDfsConstants.TtggGrupoTablePath, date)
        this.deletePartitionContent(IngestDfsConstants.TtggTiposDeCambioTablePath, date)
        this.deletePartitionContent(IngestDfsConstants.TtggPaisDivisaSedeTablePath, date)
      case Pipeline => // No se borra nada ya que son vistas que se actualizan
      case Engloba =>
        CoreRepositories.hiveRepository.sql(CoreConstants.HqlCommonTableDropPartition, EvaServiceConstants.MdEva,
          EvaServiceConstants.Engloba, s"year=${getYearFromDate(date)}, month=${getMonthFromDate(date)}, day=${getDayOfMonthFromDate(date)}")

        this.deletePartitionContent(IngestDfsConstants.EvaEnglobaTablePath, date)
      case Heffa =>
      case TablasGenerales =>
        CoreRepositories.hiveRepository.sql(CoreConstants.HqlCommonTableDropPartition, EvaServiceConstants.MdTablasGenerales,
          EvaServiceConstants.Clientes, s"year=${getYearFromDate(date)}, month=${getMonthFromDate(date)}, day=${getDayOfMonthFromDate(date)}")
        CoreRepositories.hiveRepository.sql(CoreConstants.HqlCommonTableDropPartition, EvaServiceConstants.MdTablasGenerales,
          EvaServiceConstants.ClientesInt, s"year=${getYearFromDate(date)}, month=${getMonthFromDate(date)}, day=${getDayOfMonthFromDate(date)}")
        CoreRepositories.hiveRepository.sql(CoreConstants.HqlCommonTableDropPartition, EvaServiceConstants.MdTablasGenerales,
          EvaServiceConstants.Divisas, s"year=${getYearFromDate(date)}, month=${getMonthFromDate(date)}, day=${getDayOfMonthFromDate(date)}")
        CoreRepositories.hiveRepository.sql(CoreConstants.HqlCommonTableDropPartition, EvaServiceConstants.MdTablasGenerales,
          EvaServiceConstants.TiposCambio, s"year=${getYearFromDate(date)}, month=${getMonthFromDate(date)}, day=${getDayOfMonthFromDate(date)}")
        CoreRepositories.hiveRepository.sql(CoreConstants.HqlCommonTableDropPartition, EvaServiceConstants.MdTablasGenerales,
          EvaServiceConstants.Oficinas, s"year=${getYearFromDate(date)}, month=${getMonthFromDate(date)}, day=${getDayOfMonthFromDate(date)}")

        this.deletePartitionContent(IngestDfsConstants.TtggClientesTablePath, date)
        this.deletePartitionContent(IngestDfsConstants.TtggClientesIntTablePath, date)
        this.deletePartitionContent(IngestDfsConstants.TtggDivisasTablePath, date)
        this.deletePartitionContent(IngestDfsConstants.TtggTiposCambioTablePath, date)
        this.deletePartitionContent(IngestDfsConstants.TtggOficinasTablePath, date)
    }
  }

  override def loadMaster(evaUseCase: EvaUseCase, date: Date): Unit = {
    evaUseCase match {
      case Eva =>
        // md_eva.eva -> cargamos los datos en el master e invalidamos la metadata de Impala
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlEvaLoadPartitionEva, getYearFromDate(date).toString, getMonthFromDate(date).toString, getDayOfMonthFromDate(date).toString)
        CoreRepositories.impalaRepository.invalidate(EvaServiceConstants.MdEva, EvaServiceConstants.Eva)

        // md_eva.grupoficticio -> cargamos los datos en el master e invalidamos la metadata de Impala
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlEvaLoadPartitionGrupoFicticio, getYearFromDate(date).toString, getMonthFromDate(date).toString, getDayOfMonthFromDate(date).toString)
        CoreRepositories.impalaRepository.invalidate(EvaServiceConstants.MdEva, EvaServiceConstants.Grupoficticio)

        // md_tablasgenerales.grupo -> cargamos los datos en el master e invalidamos la metadata de Impala
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlEvaLoadPartitionGrupo, getYearFromDate(date).toString, getMonthFromDate(date).toString, getDayOfMonthFromDate(date).toString)
        CoreRepositories.impalaRepository.invalidate(EvaServiceConstants.MdTablasGenerales, EvaServiceConstants.Grupo)

        // md_tablasgenerales.paisdivisasede -> cargamos los datos en el master e invalidamos la metadata de Impala
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlEvaLoadPartitionPaisDivisaSede, getYearFromDate(date).toString, getMonthFromDate(date).toString, getDayOfMonthFromDate(date).toString)
        CoreRepositories.impalaRepository.invalidate(EvaServiceConstants.MdTablasGenerales, EvaServiceConstants.PaisDivisaSede)

        // md_eva.parametro -> cargamos los datos en el master e invalidamos la metadata de Impala
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlEvaLoadPartitionParametro, getYearFromDate(date).toString, getMonthFromDate(date).toString, getDayOfMonthFromDate(date).toString)
        CoreRepositories.impalaRepository.invalidate(EvaServiceConstants.MdEva, EvaServiceConstants.Parametro)

        // md_eva.producto -> cargamos los datos en el master e invalidamos la metadata de Impala
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlEvaLoadPartitionProducto, getYearFromDate(date).toString, getMonthFromDate(date).toString, getDayOfMonthFromDate(date).toString)
        CoreRepositories.impalaRepository.invalidate(EvaServiceConstants.MdEva, EvaServiceConstants.Producto)

        // md_tablasgenerales.tiposdecambio -> cargamos los datos en el master e invalidamos la metadata de Impala
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlEvaLoadPartitionTiposDeCambio, getYearFromDate(date).toString, getMonthFromDate(date).toString, getDayOfMonthFromDate(date).toString)
        CoreRepositories.impalaRepository.invalidate(EvaServiceConstants.MdTablasGenerales, EvaServiceConstants.TiposDeCambio)
      case Pipeline =>
        // md_eva.v_tenspdov -> actualizamos la vista e invalidamos la metadata
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlEvaPipelineAlterViewTenspdov, getYearFromDate(date).toString, getMonthFromDate(date).toString, getDayOfMonthFromDate(date).toString)
        CoreRepositories.impalaRepository.invalidate(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenspdov)

        // md_eva.v_tenspdpi -> actualizamos la vista e invalidamos la metadata
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlEvaPipelineAlterViewTenspdpi, getYearFromDate(date).toString, getMonthFromDate(date).toString, getDayOfMonthFromDate(date).toString)
        CoreRepositories.impalaRepository.invalidate(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenspdpi)
      case Engloba =>
        // Cargamos los datos de Engloba en el master
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlEvaEnglobaLoadPartition, getYearFromDate(date).toString, getMonthFromDate(date).toString, getDayOfMonthFromDate(date).toString)
        // Invalidamos la metadata de Impala
        CoreRepositories.impalaRepository.invalidate(EvaServiceConstants.MdEva, EvaServiceConstants.Engloba)
      case Heffa => // Heffa es una vista directa sobre una tabla en raw
      case TablasGenerales =>
        // md_tablasgenerales.clientes -> cargamos los datos en el master e invalidamos la metadata de Impala
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlEvaTtggLoadPartitionClientes, getYearFromDate(date).toString, getMonthFromDate(date).toString, getDayOfMonthFromDate(date).toString)
        CoreRepositories.impalaRepository.invalidate(EvaServiceConstants.MdTablasGenerales, EvaServiceConstants.Clientes)

        // md_tablasgenerales.clientes_int - cargamos los datos en el master e invalidamos la metadata de Impala
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlEvaTtggLoadPartitionClientesInt, getYearFromDate(date).toString, getMonthFromDate(date).toString, getDayOfMonthFromDate(date).toString)
        CoreRepositories.impalaRepository.invalidate(EvaServiceConstants.MdTablasGenerales, EvaServiceConstants.ClientesInt)

        // md_tablasgenerales.divisas -> cargamos los datos en el master e invalidamos la metadata de Impala
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlEvaTtggLoadPartitionDivisas, getYearFromDate(date).toString, getMonthFromDate(date).toString, getDayOfMonthFromDate(date).toString)
        CoreRepositories.impalaRepository.invalidate(EvaServiceConstants.MdTablasGenerales, EvaServiceConstants.Divisas)

        // md_tablasgenerales.tipos_cambio -> cargamos los datos en el master e invalidamos la metadata de Impala
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlEvaTtggLoadPartitionTiposCambio, getYearFromDate(date).toString, getMonthFromDate(date).toString, getDayOfMonthFromDate(date).toString)
        CoreRepositories.impalaRepository.invalidate(EvaServiceConstants.MdTablasGenerales, EvaServiceConstants.TiposCambio)

        // md_tablasgenerales.oficinas -> cargamos los datos en el master e invalidamos la metadata de Impala
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlEvaTtggLoadPartitionOficinas, getYearFromDate(date).toString, getMonthFromDate(date).toString, getDayOfMonthFromDate(date).toString)
        CoreRepositories.impalaRepository.invalidate(EvaServiceConstants.MdTablasGenerales, EvaServiceConstants.Oficinas)

        // md_tablasgenerales.agrupaciones_crm_p_tfm -> actualizamos la vista e invalidamos la metadata
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlEvaTtggAlterViewAgrupacionesCrmPTfm, getYearFromDate(date).toString, getMonthFromDate(date).toString, getDayOfMonthFromDate(date).toString)
        CoreRepositories.impalaRepository.invalidate(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenjcrct)

        // md_tablasgenerales.agrupaciones -> actualizamos la vista e invalidamos la metadata
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlEvaTtggAlterViewAgrupaciones, getYearFromDate(date).toString, getMonthFromDate(date).toString, getDayOfMonthFromDate(date).toString)
        CoreRepositories.impalaRepository.invalidate(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenjcgup)

        // md_tablasgenerales.participe_grupo_crm -> actualizamos la vista e invalidamos la metadata
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlEvaTtggAlterViewParticipeGrupoCrm, getYearFromDate(date).toString, getMonthFromDate(date).toString, getDayOfMonthFromDate(date).toString)
        CoreRepositories.impalaRepository.invalidate(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenjcrhg)

        // md_tablasgenerales.participe_grupo_tfm -> actualizamos la vista e invalidamos la metadata
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlEvaTtggAlterViewParticipeGrupoTfm, getYearFromDate(date).toString, getMonthFromDate(date).toString, getDayOfMonthFromDate(date).toString)
        CoreRepositories.impalaRepository.invalidate(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenjctdf)

        // md_tablasgenerales.participes -> actualizamos la vista e invalidamos la metadata
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlEvaTtggAlterViewParticipes, getYearFromDate(date).toString, getMonthFromDate(date).toString, getDayOfMonthFromDate(date).toString)
        CoreRepositories.impalaRepository.invalidate(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenjcktl)

        // md_tablasgenerales.rela_agrupaciones -> actualizamos la vista e invalidamos la metadata
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlEvaTtggAlterViewRelaAgrupaciones, getYearFromDate(date).toString, getMonthFromDate(date).toString, getDayOfMonthFromDate(date).toString)
        CoreRepositories.impalaRepository.invalidate(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenjcrkg)

        // md_tablasgenerales.tipo_agrupacion -> actualizamos la vista e invalidamos la metadata
        CoreRepositories.hiveRepository.sql(IngestConstants.HqlEvaTtggAlterViewTipoAgrupacion, getYearFromDate(date).toString, getMonthFromDate(date).toString, getDayOfMonthFromDate(date).toString)
        CoreRepositories.impalaRepository.invalidate(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenjctag)
    }
  }

  override def auditTables(evaUseCase: EvaUseCase, date: Date): Unit = {
    evaUseCase match {
      case Eva =>
        auditTable(IngestConstants.HqlEvaAuditEva, date)
        auditTable(IngestConstants.HqlEvaAuditGrupo, date)
        auditTable(IngestConstants.HqlEvaAuditGrupoFicticio, date)
        auditTable(IngestConstants.HqlEvaAuditPaisDivisaSede, date)
        auditTable(IngestConstants.HqlEvaAuditParametro, date)
        auditTable(IngestConstants.HqlEvaAuditProducto, date)
        auditTable(IngestConstants.HqlEvaAuditTiposDeCambio, date)
      case Pipeline =>
        auditTable(IngestConstants.HqlEvaPipelineAuditTenspdov, date)
        auditTable(IngestConstants.HqlEvaPipelineAuditTenspdpi, date)
      case Engloba =>
        auditTable(IngestConstants.HqlEvaAuditEngloba, date)
      case Heffa => // Nada que auditar
      case TablasGenerales =>
        auditTable(IngestConstants.HqlEvaTtggAuditClientes, date)
        auditTable(IngestConstants.HqlEvaTtggAuditClientesInt, date)
        auditTable(IngestConstants.HqlEvaTtggAuditDivisas, date)
        auditTable(IngestConstants.HqlEvaTtggAuditTiposCambio, date)
        auditTable(IngestConstants.HqlEvaTtggAuditOficinas, date)
    }
    CoreRepositories.impalaRepository.invalidate(EvaServiceConstants.RdEbdmau, EvaServiceConstants.TAuditMaster)
  }

  override def deleteFlags(evaUseCase: EvaUseCase): Unit = {
    evaUseCase match {
      case Eva =>
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tendvehs)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tendvtcb)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenwfduc)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tentgges)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenydgup)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenydytp)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenydier)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenydpai)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenydyps)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenydyss)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenydxed)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenydcrs)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenydiur)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenwfgfi)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenwfkec)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenwfpec)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenwfpco)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenbtctr)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tentgtcm)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tentgytr)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tentgdiv)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenydmgl)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenydmrg)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenydfec)
      case Pipeline =>
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenspdov)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenspdpi)
      case Engloba =>
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tendgcel)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tendgcir)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tendgehr)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tendggur)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tendgvdp)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tendgwbp)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tendgwje)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tendgwlr)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tendgwne)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tendgwpa)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tendgwqr)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Vendgeh1)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Vendgwn1)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Vendgwp1)
      case Heffa =>
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tendsrfq)
      case TablasGenerales =>
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenyddcb)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenydgvc)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tentgdvs)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Ventgtcm1)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenorgru)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenjcgup)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenjcrkg)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenjctag)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenjcktl)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenjcrhg)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenjctdf)
        IngestServices.commonService.deleteRawFlag(EvaServiceConstants.RdInformacional, EvaServiceConstants.Tenjcrct)
    }
  }

  /**
    * Borra el contenido de una partición
    *
    * @param tablePath - path a la tabla
    * @param date - fecha (para obtener la partición)
    */
  private def deletePartitionContent(tablePath: String, date: Date): Unit = {
      CoreRepositories.dfsRepository.deleteContent(s"$tablePath" +
        s"${CoreConstants.slash}year=${getYearFromDate(date)}" +
        s"${CoreConstants.slash}month=${getMonthFromDate(date)}" +
        s"${CoreConstants.slash}day=${getDayOfMonthFromDate(date)}")
  }

  /**
    * Auditoría de la tabla
    *
    * @param hql - hql a ejecutar para la auditoría
    * @param date - fecha de ejecución
    */
  private def auditTable(hql: String, date: Date) {
    val fechaCarga = EvaServiceConstants.format1.format(date)
    val fechaMillis = EvaServiceConstants.format2.format(new Date)
    CoreRepositories.hiveRepository.sql(hql,
      fechaCarga, getYearFromDate(date).toString, getMonthFromDate(date).toString, getDayOfMonthFromDate(date).toString,
      fechaMillis, CoreAppInfo.uuid, CoreAppInfo.malla, CoreAppInfo.job, LinxAppArgs.user, LinxAppArgs.appName)
  }

  /**
    * Devuelve el año de un objeto Date
    *
    * @param date - fecha pasada
    * @return - entero con el año extraído
    */
  private def getYearFromDate(date: Date): Int = {
    val cal = Calendar.getInstance
    cal.setTime(date)
    cal.get(Calendar.YEAR)
  }

  /**
    * Devuelve el mes de un objeto Date
    *
    * @param date - fecha pasada
    * @return - entero con el mes extraído
    */
  private def getMonthFromDate(date: Date): Int = {
    val cal = Calendar.getInstance
    cal.setTime(date)
    cal.get(Calendar.MONTH) + 1
  }

  /**
    * Devuelve el día del mes de un objeto Date
    *
    * @param date - fecha pasada
    * @return - entero con el día del mes extraído
    */
  private def getDayOfMonthFromDate(date: Date): Int = {
    val cal = Calendar.getInstance
    cal.setTime(date)
    cal.get(Calendar.DAY_OF_MONTH)
  }
}
