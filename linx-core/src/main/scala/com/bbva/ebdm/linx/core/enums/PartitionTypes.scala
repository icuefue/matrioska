package com.bbva.ebdm.linx.core.enums

object PartitionTypes {
  sealed trait PartitionType
  case object NoPartition extends PartitionType
  case object Fechaproceso extends PartitionType
  case object YearMonthDay extends PartitionType

  def getObject(partitionType: String): PartitionType = {
    partitionType match {
      case "NO_PARTITION" => NoPartition
      case "FECHAPROCESO" => Fechaproceso
      case "YEAR_MONTH_DAY" => YearMonthDay
      case _ => NoPartition
    }
  }
}