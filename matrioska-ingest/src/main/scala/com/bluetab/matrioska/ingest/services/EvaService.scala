package com.bluetab.matrioska.ingest.services

import java.util.Date

import com.bluetab.matrioska.ingest.apps.EvaUseCases.EvaUseCase

/**
  * Interfaz o Trait con las servicios comunes del proyecto EVA*
  * @author xe58268
  *
  */

trait EvaService {

//  /**
//    * Borrado de las tablas del caso de uso especificado y las particiones de la fecha actual si existen
//    *
//    * @param evaUseCase - caso de uso
//    */
  def deleteTablesAndPartitions(evaUseCase: EvaUseCase, date: Date): Unit

  /**
    * Carga de las tablas o vistas del master del caso de uso especificado
    *
    * @param evaUseCase - caso de uso
    * @param date - fecha de carga
    */
  def loadMaster(evaUseCase: EvaUseCase, date: Date): Unit

  /**
    * Auditoría de las tablas del caso de uso especificado
    *
    * @param evaUseCase - caso de uso
    */
  def auditTables(evaUseCase: EvaUseCase, date: Date): Unit

  /**
    * Borramos los flags del caso de uso especificado
    *
    * @param evaUseCase - caso de uso
    */
  def deleteFlags(evaUseCase: EvaUseCase): Unit
}