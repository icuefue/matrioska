package com.bbva.ebdm.linx.core.repositories

import com.bbva.ebdm.linx.core.beans.RdbmsOptions
import com.bbva.ebdm.linx.core.enums.PartitionTypes.PartitionType


/**
 * Acciones para importar o exportar bases de datos relacionales a Hdfs
 *
 * @author xe54068
 */
trait RdbmsRepository {

  /**
   * Método para importar tablas de un sistema relacional de base de datos via Sqoop
   *
   * @param options: se definen la información necesaria para importar una tabla de una base de datos relacional
   *                 a una tabla Hive del cluster
   */
  def importTool(options: RdbmsOptions)

//  /**
//   * Construcción del targetDir de partición a partir del tipo de partición pasado
//   *
//   * @param partitionType - tipo de partición
//   * @param baseDir       - dirección base (directorio donde reside la tabla)
//   * @param planDate      - Fecha de planificación
//   * @return String con el targetDir de esa partición
//   */
//  def buildPartitionTargetDir(partitionType: PartitionType, baseDir: String, planDate: String): String
}
