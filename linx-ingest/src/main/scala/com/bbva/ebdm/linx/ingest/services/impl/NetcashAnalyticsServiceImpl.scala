package com.bbva.ebdm.linx.ingest.services.impl

import com.bbva.ebdm.linx.core.conf.{CoreContext, CoreRepositories, CoreServices}
import com.bbva.ebdm.linx.ingest.conf.IngestServices
import com.bbva.ebdm.linx.ingest.constants.IngestConstants
import com.bbva.ebdm.linx.ingest.services.NetcashAnalyticsService

object NetcashAnalyticsServiceConstants {
  val RdSstt = "rd_sstt"
  val RdNetcash = "rd_netcash"
  val RdInformacional = "rd_informacional"

  val TReferenciaExternaCliente = "t_referencia_externa_cliente"
  val NombreVistaContratos = "Contratos"

  val TServiciosContratados = "t_servicios_contratados"
  val NombreVistaServiciosContratados = "Servicios contratados"

  val TServiciosNormalizadosContratados  = "t_servicios_normalizados_contratados"
  val NombreVistaNormalizador = "Normalizador"

  val TCatalogoProductos  = "t_catalogo_productos"
  val NombreVistaCatalogoCanales = "Catalogo canales"

  val TCatalogoServicios  = "t_catalogo_servicios"
  val NombreVistaCatalogoServicios = "Catálogo servicios"

  val TSituacionFicheros = "t_situacion_ficheros"
  val NombreVistaCatalogoSituacionFicheros = "Catálogo situación ficheros"

  val TFormatoFicheros = "t_formato_ficheros"
  val NombreVistaCatalogoFormatoFicheros = "Catálogo formato ficheros"

  val TAdministracionUsuarios = "t_administracion_usuarios"
  val NombreVistaUsuarios = "Usuarios"

  val TPerfilCliente = "t_perfil_cliente"
  val NombreVistaPerfilCliente = "Perfil Cliente"

  // Solamente para borrar el flag
  val TUsoServicios = "t_uso_servicios"
  val TUsoFicheros = "t_uso_ficheros"
  val Tenspnca = "tenspnca"
}

/**
  * Created by xe54068 on 12/01/2017.
  */
class NetcashAnalyticsServiceImpl extends NetcashAnalyticsService {

  override def alterViewContratos(): Unit = {
    val result = CoreServices.commonService.getMaxPartition(  NetcashAnalyticsServiceConstants.RdSstt,
                                                              NetcashAnalyticsServiceConstants.TReferenciaExternaCliente)
    result match {
      case Some(result_some) =>
        alterView(NetcashAnalyticsServiceConstants.NombreVistaContratos, IngestConstants.HqlNetcashAnalyticsAlterViewContratos, result_some)
      case None =>
        notPartitionsFound(NetcashAnalyticsServiceConstants.NombreVistaContratos)
    }
  }

  override def alterViewServiciosContratados(): Unit = {
    val result = CoreServices.commonService.getMaxPartition(  NetcashAnalyticsServiceConstants.RdSstt,
      NetcashAnalyticsServiceConstants.TServiciosContratados)
    result match {
      case Some(result_some) =>
        alterView(NetcashAnalyticsServiceConstants.NombreVistaServiciosContratados, IngestConstants.HqlNetcashAnalyticsAlterViewServiciosContratados, result_some)
      case None =>
        notPartitionsFound(NetcashAnalyticsServiceConstants.NombreVistaServiciosContratados)
    }
  }

  override  def alterViewNormalizador(): Unit = {
    val result = CoreServices.commonService.getMaxPartition(  NetcashAnalyticsServiceConstants.RdSstt,
      NetcashAnalyticsServiceConstants.TServiciosNormalizadosContratados)
    result match {
      case Some(result_some) =>
        alterView(NetcashAnalyticsServiceConstants.NombreVistaNormalizador, IngestConstants.HqlNetcashAnalyticsAlterViewNormalizador, result_some)
      case None =>
        notPartitionsFound(NetcashAnalyticsServiceConstants.NombreVistaNormalizador)
    }
  }

  override def alterViewCatalogoCanales(): Unit = {
    val result = CoreServices.commonService.getMaxPartition(  NetcashAnalyticsServiceConstants.RdSstt,
      NetcashAnalyticsServiceConstants.TCatalogoProductos)
    result match {
      case Some(result_some) =>
        alterView(NetcashAnalyticsServiceConstants.NombreVistaCatalogoCanales, IngestConstants.HqlNetcashAnalyticsAlterViewCatalogoCanales, result_some)
      case None =>
        notPartitionsFound(NetcashAnalyticsServiceConstants.NombreVistaCatalogoCanales)
    }
  }

  override  def alterViewCatalogoServicios(): Unit = {
    val result = CoreServices.commonService.getMaxPartition(  NetcashAnalyticsServiceConstants.RdSstt,
      NetcashAnalyticsServiceConstants.TCatalogoServicios)
    result match {
      case Some(result_some) =>
        alterView(NetcashAnalyticsServiceConstants.NombreVistaCatalogoServicios, IngestConstants.HqlNetcashAnalyticsAlterViewCatalogoServicios, result_some)
      case None =>
        notPartitionsFound(NetcashAnalyticsServiceConstants.NombreVistaCatalogoServicios)
    }
  }

  override def alterViewCatalogoSituacionFicheros(): Unit = {
    val result = CoreServices.commonService.getMaxPartition(  NetcashAnalyticsServiceConstants.RdSstt,
                                                              NetcashAnalyticsServiceConstants.TSituacionFicheros)
    result match {
      case Some(result_some) =>
        alterView(NetcashAnalyticsServiceConstants.NombreVistaCatalogoSituacionFicheros, IngestConstants.HqlNetcashAnalyticsAlterViewCatalogoSituacionFicheros, result_some)
      case None =>
        notPartitionsFound(NetcashAnalyticsServiceConstants.NombreVistaCatalogoSituacionFicheros)
    }
  }

  override def alterViewCatalogoFormatoFicheros(): Unit = {
    val result = CoreServices.commonService.getMaxPartition(  NetcashAnalyticsServiceConstants.RdSstt,
      NetcashAnalyticsServiceConstants.TFormatoFicheros)
    result match {
      case Some(result_some) =>
        alterView(NetcashAnalyticsServiceConstants.NombreVistaCatalogoFormatoFicheros, IngestConstants.HqlNetcashAnalyticsAlterViewCatalogoFormatoFicheros, result_some)
      case None =>
        notPartitionsFound(NetcashAnalyticsServiceConstants.NombreVistaCatalogoFormatoFicheros)
    }
  }

  override def alterViewUsuarios(): Unit = {
    val result = CoreServices.commonService.getMaxPartition(  NetcashAnalyticsServiceConstants.RdNetcash,
      NetcashAnalyticsServiceConstants.TAdministracionUsuarios)
    result match {
      case Some(result_some) =>
        alterView(NetcashAnalyticsServiceConstants.NombreVistaUsuarios, IngestConstants.HqlNetcashAnalyticsAlterViewUsuarios, result_some)
      case None =>
        notPartitionsFound(NetcashAnalyticsServiceConstants.NombreVistaUsuarios)
    }
  }

  override def alterViewPerfilCliente(): Unit = {
    val result = CoreServices.commonService.getMaxPartition(  NetcashAnalyticsServiceConstants.RdNetcash,
      NetcashAnalyticsServiceConstants.TPerfilCliente)
    result match {
      case Some(result_some) =>
        alterView(NetcashAnalyticsServiceConstants.NombreVistaPerfilCliente, IngestConstants.HqlNetcashAnalyticsAlterViewPerfilCliente, result_some)
      case None =>
        notPartitionsFound(NetcashAnalyticsServiceConstants.NombreVistaPerfilCliente)
    }
  }

  private def alterView(name: String, hql: String, date: String): Unit = {

    CoreContext.logger.info("date: " + date)

    CoreContext.logger.info(s"Actualizar vista '$name' para que apunte a la partición: " +
      s"year = ${date.substring(0,4)}" +
      s", month = ${date.substring(4,6)} " +
      s", day = ${date.substring(6,8)}")

    CoreRepositories.hiveRepository.sql(  hql,
      date.substring(0,4),
      date.substring(4,6),
      date.substring(6,8))
  }

  private def notPartitionsFound(name: String): Unit = {
    CoreContext.logger.info(s"No se han encontrado particiones en la vista '$name'")
  }

  override def deleteFlags(): Unit = {

    // Flags de SSTT
    IngestServices.commonService.deleteRawFlag(NetcashAnalyticsServiceConstants.RdSstt, NetcashAnalyticsServiceConstants.TReferenciaExternaCliente)
    IngestServices.commonService.deleteRawFlag(NetcashAnalyticsServiceConstants.RdSstt, NetcashAnalyticsServiceConstants.TServiciosContratados)
    IngestServices.commonService.deleteRawFlag(NetcashAnalyticsServiceConstants.RdSstt, NetcashAnalyticsServiceConstants.TServiciosNormalizadosContratados)
    IngestServices.commonService.deleteRawFlag(NetcashAnalyticsServiceConstants.RdSstt, NetcashAnalyticsServiceConstants.TCatalogoProductos)
    IngestServices.commonService.deleteRawFlag(NetcashAnalyticsServiceConstants.RdSstt, NetcashAnalyticsServiceConstants.TCatalogoServicios)
    IngestServices.commonService.deleteRawFlag(NetcashAnalyticsServiceConstants.RdSstt, NetcashAnalyticsServiceConstants.TUsoFicheros)
    IngestServices.commonService.deleteRawFlag(NetcashAnalyticsServiceConstants.RdSstt, NetcashAnalyticsServiceConstants.TSituacionFicheros)
    IngestServices.commonService.deleteRawFlag(NetcashAnalyticsServiceConstants.RdSstt, NetcashAnalyticsServiceConstants.TFormatoFicheros)

    // Flags de Netcash
    IngestServices.commonService.deleteRawFlag(NetcashAnalyticsServiceConstants.RdNetcash, NetcashAnalyticsServiceConstants.TAdministracionUsuarios)
    IngestServices.commonService.deleteRawFlag(NetcashAnalyticsServiceConstants.RdNetcash, NetcashAnalyticsServiceConstants.TUsoServicios)
    IngestServices.commonService.deleteRawFlag(NetcashAnalyticsServiceConstants.RdNetcash, NetcashAnalyticsServiceConstants.TPerfilCliente)

    // Flags de Mat
    IngestServices.commonService.deleteRawFlag(NetcashAnalyticsServiceConstants.RdInformacional, NetcashAnalyticsServiceConstants.Tenspnca)
  }
}
