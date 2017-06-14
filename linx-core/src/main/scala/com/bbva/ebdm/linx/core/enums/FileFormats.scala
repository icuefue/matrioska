package com.bbva.ebdm.linx.core.enums

import com.cloudera.sqoop.SqoopOptions.FileLayout
import com.bbva.ebdm.linx.core.constants.CoreConstants
import com.bbva.ebdm.linx.core.conf.CoreRepositories
import com.bbva.ebdm.linx.core.enums.PartitionTypes.PartitionType
import com.bbva.ebdm.linx.core.enums.PartitionTypes.NoPartition
import com.bbva.ebdm.linx.core.enums.PartitionTypes.Fechaproceso
import com.bbva.ebdm.linx.core.enums.PartitionTypes.YearMonthDay

object FileFormats {
  sealed trait FileFormat {def fileLayout: FileLayout}
  case object Text extends FileFormat {val fileLayout = FileLayout.TextFile}
  case object Parquet extends FileFormat {val fileLayout = FileLayout.ParquetFile}
  case object Avro extends FileFormat {val fileLayout = FileLayout.AvroDataFile}
  case object Sequence extends FileFormat {val fileLayout = FileLayout.SequenceFile}

  def getObject(fileFormat: String): Option[FileFormat] = {
    fileFormat match {
      case "TEXT" => Some(Text)
      case "PARQUET" => Some(Parquet)
      case "AVRO" => Some(Avro)
      case "SEQUENCE" => Some(Sequence)
      case _ => None
    }
  }
  val fileFormats = Seq(Text, Parquet, Avro, Sequence)


  val partitionTypes = Seq(NoPartition, Fechaproceso, YearMonthDay)
}

