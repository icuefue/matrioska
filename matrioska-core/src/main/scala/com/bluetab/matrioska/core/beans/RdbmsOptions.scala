package com.bluetab.matrioska.core.beans

import com.bluetab.matrioska.core.enums.CompressionCodecs.CompressionCodec
import com.bluetab.matrioska.core.enums.FileFormats.FileFormat
import com.bluetab.matrioska.core.enums.PartitionTypes.PartitionType





case class RdbmsOptions(
                         source: String,
                         sourceSchema: String,
                         sourceTable: String,
                         targetSchema: String,
                         targetTable: String,
                         targetDir: String,
                         partitionType: PartitionType,
                         fieldsTerminatedBy: Char,
                         linesTerminatedBy: Char,
                         planDate: String,
                         fileFormat: Option[FileFormat],
                         compressionCodec: Option[CompressionCodec],
                         importToPreraw: Boolean,
                         enclosedByQuotes: Boolean,
                         escapedByBackslash: Boolean) {

  override def toString: String = {
    s"$source, $sourceSchema.$sourceTable => $targetSchema.$targetTable ($targetDir) ---- partitionType -> ${partitionType.toString}" +
      s", field delimiter -> ${Integer.toHexString(fieldsTerminatedBy.toInt)}, line delimiter -> ${Integer.toHexString(linesTerminatedBy)}, " +
      s"fileFormat -> ${fileFormat.toString}, compressionCodec -> ${compressionCodec.toString}, importToPreraw -> $importToPreraw, " +
      s"enclosedByQuotes -> $enclosedByQuotes, escapedByBackslash -> $escapedByBackslash"
}

}
