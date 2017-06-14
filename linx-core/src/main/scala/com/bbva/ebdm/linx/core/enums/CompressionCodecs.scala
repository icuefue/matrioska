package com.bbva.ebdm.linx.core.enums

object CompressionCodecs {
  sealed trait CompressionCodec { def codecLib: String }
  case object Snappy extends CompressionCodec { val codecLib = "org.apache.hadoop.io.compress.SnappyCodec" }
  case object Gzip extends CompressionCodec { val codecLib = "org.apache.hadoop.io.compress.GzipCodec" }
  case object Lzo extends CompressionCodec { val codecLib = "org.apache.hadoop.io.compress.LzoCodec" }

  def getObject(compressionCodec: String): Option[CompressionCodec] = {
    compressionCodec match {
      case "SNAPPY" => Some(Snappy)
      case "GZIP" => Some(Gzip)
      case "Lzo" => Some(Lzo)
      case _ => None
    }
  }

  val compressionCodecs = Seq(Snappy, Gzip, Lzo)
}