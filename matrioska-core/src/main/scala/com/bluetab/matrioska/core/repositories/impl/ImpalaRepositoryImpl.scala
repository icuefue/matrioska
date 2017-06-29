package com.bluetab.matrioska.core.repositories.impl

import com.bluetab.matrioska.core.conf.{CoreConfig, CoreContext}
import com.bluetab.matrioska.core.repositories.ImpalaRepository

class ImpalaRepositoryImpl extends ImpalaRepository {

  def invalidate(schema: String) {
    val realSchema = CoreConfig.hive.schemas.get(schema)
    execute(s"INVALIDATE METADATA $realSchema")
  }

  def invalidate(schema: String, table: String) {
    CoreContext.logger.debug(s"ImpalaRepositoryImpl -> invalidate $schema.$table")
    val realSchema = CoreConfig.hive.schemas.get(schema)
    CoreContext.logger.debug(s"ImpalaRepositoryImpl (realSchema) -> invalidate $realSchema.$table")
    execute(s"INVALIDATE METADATA $realSchema.$table")
  }

  def refresh(schema: String, table: String) {
    val realSchema = CoreConfig.hive.schemas.get(schema)
    execute(s"REFRESH $realSchema.$table")
  }

  def computeStats(schema: String, table: String) {
    val realSchema = CoreConfig.hive.schemas.get(schema)
    execute(s"COMPUTE STATS $realSchema.$table")
  }

  private def execute(command: String) {
    
    val statement = CoreContext.impalaConnection.get.createStatement
    statement.execute(command)
     
    
    CoreContext.logger.debug(s"IMPALA: $command")
  }

}
