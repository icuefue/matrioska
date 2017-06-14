package com.bbva.ebdm.linx.ingest.apps

import com.bbva.ebdm.linx.core.LinxApp
import com.bbva.ebdm.linx.ingest.conf.IngestServices

/**
  * Aprovisionamiento de datos en el master de Netcash analytics
  *
  * Created by xe54068 on 12/01/2017.
  */
class NetcashAnalyticsApp extends LinxApp {
  override def run(args: Seq[String]): Unit = {

    // Actualizamos la vista 'Contratos'
    IngestServices.netcashAnalyticsService.alterViewContratos()

    // Actualizamos la vista 'Servicios contratados'
    //IngestServices.netcashAnalyticsService.alterViewServiciosContratados()

    // Actualizamos la vista 'Normalizador'
    IngestServices.netcashAnalyticsService.alterViewNormalizador()

    // Actualizamos la vista 'Catalogo canales'
    IngestServices.netcashAnalyticsService.alterViewCatalogoCanales()

    // Actualizamos la vista 'Catalogo servicios'
    IngestServices.netcashAnalyticsService.alterViewCatalogoServicios()

    // Actualizamos la vista 'Catalogo situacion ficheros'
    IngestServices.netcashAnalyticsService.alterViewCatalogoSituacionFicheros()

    // Actualizamos la vista 'Catalogo formato ficheros'
    IngestServices.netcashAnalyticsService.alterViewCatalogoFormatoFicheros()

    // Actualizamos la vista 'Usuarios'
    //IngestServices.netcashAnalyticsService.alterViewUsuarios()

    // Actualizamos la vista 'Perfil cliente'
    //IngestServices.netcashAnalyticsService.alterViewPerfilCliente()

    // Borramos los flags
    IngestServices.netcashAnalyticsService.deleteFlags()
  }
}
