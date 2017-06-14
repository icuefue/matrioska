package com.bluetab.matrioska.core.enums

import com.cloudera.sqoop.SqoopOptions.FileLayout

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
  val fileFormats:Seq[FileFormat] = Seq(Text, Parquet, Avro, Sequence)


  val partitionTypes:Seq[PartitionType] = Seq(NoPartition, Fechaproceso, YearMonthDay)
}

