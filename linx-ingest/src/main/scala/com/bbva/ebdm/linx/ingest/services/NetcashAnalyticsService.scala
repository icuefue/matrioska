package com.bbva.ebdm.linx.ingest.services

import java.util.Date

/**
  * Interfaz o Trait con los servicios comunes del proyecto Netcash Analytics
  *
  * Created by xe54068 on 12/01/2017.
  */
trait NetcashAnalyticsService {

  /**
    * Recrear la vista "contratos" del master
    */
  def alterViewContratos()

  /**
    * Recrear la vista "servicios_contratados" del master
    */
  def alterViewServiciosContratados()

  /**
    * Recrear la vista "normalizador" del master
    */
  def alterViewNormalizador()

  /**
    * Recrear la vista "catalogo_canales" del master
    */
  def alterViewCatalogoCanales()

  /**
    * Recrear la vista "catalogo_servicios" del master
    */
  def alterViewCatalogoServicios()

  /**
    * Recrear la vista "catalogo_situacion_ficheros" del master
    */
  def alterViewCatalogoSituacionFicheros()

  /**
    * Recrear la vista "catalogo_formato_ficheros" del master
    */
  def alterViewCatalogoFormatoFicheros()

  /**
    * Recrear la vista "usuarios" del master
    */
  def alterViewUsuarios()

  /**
    * Recrear la vista "perfil_cliente" del master
    */
  def alterViewPerfilCliente()

  /**
    * Borrado de flags
    */
  def deleteFlags()
}
