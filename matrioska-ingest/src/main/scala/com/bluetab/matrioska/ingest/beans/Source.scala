package com.bluetab.matrioska.ingest.beans

import com.bluetab.matrioska.core.conf.CoreConfig
import com.bluetab.matrioska.core.enums.CompressionCodecs.CompressionCodec
import com.bluetab.matrioska.core.enums.FileFormats.FileFormat
import com.bluetab.matrioska.ingest.utils.FaultToleranceTests.FaultToleranceTest

object FileTypeEnum extends Enumeration {
  type FileTypeEnum = Value
  val DelimitedFile, FixedLengthFile, SqoopCsvFile, AllInOneFieldFile, NotSpecifiedFile = Value
  def getValue(fileType: String): FileTypeEnum = {
    fileType match {
      case "DF" => DelimitedFile
      case "FF" => FixedLengthFile
      case "SC" => SqoopCsvFile
      case "TF" => AllInOneFieldFile
      case _ => NotSpecifiedFile
    }
  }
}

class Source {
  var id: String = ""
  var checkQA: Boolean = false
  var masks: Seq[Mask] = Seq[Mask]()
  var stagingPaths: Seq[String] = Seq[String]()
  var stagingExceptions: Seq[String] = Seq[String]()
  def this(id: String, checkQA: Boolean, masks: Seq[Mask]) {
    this()
    this.id = id
    this.checkQA = checkQA
    this.masks = masks
  }
}

class Mask {
  var codMask: Int = 0
  var mask: String = ""
  var checkQA: Boolean = false
  var active: Boolean = false
  var fileType: FileType = _
  var table: Table = _
  var faultToleranceTests: Seq[FaultToleranceTest] = Seq[FaultToleranceTest]()

  def this(codMask: Int, mask: String, checkQA: Boolean, active: Boolean, fileType: FileType, table: Table, faultToleranceTests: Seq[FaultToleranceTest]) {
    this()
    this.codMask = codMask
    this.mask = mask
    this.checkQA = checkQA
    this.active = active
    this.fileType = fileType
    this.table = table
    this.faultToleranceTests = faultToleranceTests
  }
}

trait FileType {
  val dateFormat: String
}

case class FixedLengthFileType(linePattern: String, header: Int, dateFormat: String) extends FileType
case class SqoopCsvFileType(fieldDelimiter: String, lineDelimiter: String, enclosedBy: String, escapedBy: String, header: Int, dateFormat: String) extends FileType
case class DelimitedFileType(fieldDelimiter: String, lineDelimiter: String, endsWithDelimiter: Boolean, header: Int, dateFormat: String) extends FileType
case class AllInOneFieldFileType(dateFormat: String) extends FileType
case class Table(schema: String, table: String, defaultPartitionning: Boolean, format: Option[FileFormat], compressionCodec: Option[CompressionCodec]) {

  def getFullName: String = {

    CoreConfig.hive.schemas.get(schema) + "." + table
  }

}

