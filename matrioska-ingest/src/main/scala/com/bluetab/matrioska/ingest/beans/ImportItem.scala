package com.bluetab.matrioska.ingest.beans

import com.bluetab.matrioska.core.enums.CompressionCodecs.CompressionCodec
import com.bluetab.matrioska.core.enums.FileFormats.FileFormat
import com.bluetab.matrioska.core.enums.PartitionTypes.PartitionType




/**
  * Objeto importable desde un origen relacional
  *
  * Created by xe54068 on 27/01/2017.
  */
case class ImportItem(
                       codItem: Int,
                       codUsecase: Int,
                       source: String,
                       sourceSchema: String,
                       sourceTable: String,
                       targetSchema: String,
                       targetTable: String,
                       targetDir: String,
                       partitionType: PartitionType,
                       fieldDelimiter: String,
                       fieldDelimiterHex: String,
                       lineDelimiter: String,
                       lineDelimiterHex: String,
                       fileFormat: Option[FileFormat],
                       compressionCodec: Option[CompressionCodec],
                       generateFlag: Boolean,
                       importToPreraw: Boolean,
                       enclosedByQuotes: Boolean,
                       escapedByBackslash: Boolean) {

  override def toString: String = {
   s"$source, $sourceSchema.$sourceTable => $targetSchema.$targetTable ($targetDir) ---- partitionType -> ${partitionType.toString}" +
     s", field delimiter -> $fieldDelimiter, line delimiter -> $lineDelimiter, fileFormat -> ${fileFormat.toString}" +
     s", compressionCodec -> ${compressionCodec.toString}, generateFlag -> $generateFlag, importToPreraw -> $importToPreraw" +
     s", enclosedByQuotes -> $enclosedByQuotes, escapedByBackslash -> $escapedByBackslash"
  }

}

