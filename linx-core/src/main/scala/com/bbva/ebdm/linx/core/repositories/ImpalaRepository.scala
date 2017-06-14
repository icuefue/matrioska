package com.bbva.ebdm.linx.core.repositories

trait ImpalaRepository {

  def invalidate(schema: String)

  def invalidate(schema: String, table: String)

  def refresh(schema: String, table: String)

  def computeStats(schema: String, table: String)

}
