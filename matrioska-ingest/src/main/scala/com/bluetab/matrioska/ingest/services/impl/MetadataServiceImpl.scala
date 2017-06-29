package com.bluetab.matrioska.ingest.services.impl

import com.bluetab.matrioska.core.conf.CoreContext
import com.bluetab.matrioska.ingest.beans.ImportItem
import com.bluetab.matrioska.core.enums.PartitionTypes
import com.bluetab.matrioska.ingest.beans.FileTypeFactory
import com.bluetab.matrioska.core.enums.FileFormats
import com.bluetab.matrioska.ingest.beans.FileTypeEnum
import com.bluetab.matrioska.ingest.beans.Source
import com.bluetab.matrioska.ingest.beans.Table
import com.bluetab.matrioska.ingest.utils.FaultToleranceTests.FaultToleranceTest
import com.bluetab.matrioska.ingest.beans.Mask
import com.bluetab.matrioska.core.conf.CoreRepositories
import com.bluetab.matrioska.core.enums.{CompressionCodecs, FileFormats, PartitionTypes}
import com.bluetab.matrioska.ingest.beans.{FileTypeBean, FileTypeFactory, ImportItem}
import com.bluetab.matrioska.ingest.constants.IngestConstants
import com.bluetab.matrioska.ingest.services.MetadataService
import com.bluetab.matrioska.ingest.utils.FaultToleranceTests

class MetadataServiceImpl extends MetadataService {

  def getStagingPaths: Seq[String] = {
    val paths = CoreRepositories.hiveRepository.sql(IngestConstants.HqlMetadataGetStagingPaths)
    paths match {
      case Some(paths) =>
        paths.map(x => x.getString(0)).collect().toSeq
      case None => Seq()
    }

  }

  def loadSources: Map[String, Source] = {

    val result = scala.collection.mutable.Map[String, Source]()
    var sources = CoreRepositories.hiveRepository.sql(IngestConstants.HqlMetadataGetSources)
    sources match {
      case Some(sources) =>
        sources.collect().foreach { row =>
          val sourceName = row.getAs[String]("dir_name")
          val checkQa1 = row.getAs[Boolean]("check_qa1")
          val codMask = row.getAs[Int]("cod_mask")
          val maskValue = row.getAs[String]("mask")
          val checkQa2 = row.getAs[Boolean]("check_qa2")
          val tableName = row.getAs[String]("table_name")
          val schemaName = row.getAs[String]("schema_name")
          val defaultPartitioning = row.getAs[Boolean]("default_partitionning")
          val fileFormat = row.getAs[String]("file_format")
          val compressionCodec = row.getAs[String]("compression_codec")
          val fileTypeValue = row.getAs[String]("file_type")
          val fieldDelimiter = pipeDelimeter(row.getAs[String]("field_delimiter"))
          val lineDelimiter = row.getAs[String]("line_delimiter")
          val endsWithDelimiter = row.getAs[Boolean]("ends_with_delimiter")
          val header = row.getAs[Int]("header")
          val dateFormat = row.getAs[String]("date_format")
          val linePattern = row.getAs[String]("line_pattern")
          val enclosedBy = row.getAs[String]("enclosed_by")
          val escapedBy = row.getAs[String]("escaped_by")
          val active = row.getAs[Boolean]("active")
          val codTolerance = row.getAs[Int]("cod_tolerance")
          val toleranceParameters = row.getAs[String]("tolerance_parameters")

          var source = result.get(sourceName)
          source match {
            case Some(pp) =>
            case None =>
              source = Some(new Source(sourceName, checkQa1, Seq[Mask]()))
              result.put(sourceName, source.get)
          }
          if (codMask != 0) {
            var mask: Option[Mask] = None
            for (m <- source.get.masks) {
              if (m.codMask == codMask) mask = Some(m)
            }
            mask match {
              case Some(pp) =>
              case None =>
                val table = Table(schemaName, tableName, defaultPartitioning, FileFormats.getObject(fileFormat), CompressionCodecs.getObject(compressionCodec))
                val fileTypeBean = FileTypeBean(FileTypeEnum.getValue(fileTypeValue), fieldDelimiter, lineDelimiter,
                  endsWithDelimiter, header, dateFormat, linePattern, enclosedBy, escapedBy)
                val fileType = FileTypeFactory(fileTypeBean)
                mask = Some(new Mask(codMask, maskValue, checkQa2, active, fileType, table, Seq[FaultToleranceTest]()))

                source.get.masks = source.get.masks :+ mask.get
            }
            var tolerance: Option[FaultToleranceTest] = None
            if (codTolerance != 0)
              tolerance = FaultToleranceTests.getFaultToleranceTest(codTolerance, toleranceParameters)
            tolerance match {
              case Some(tolerance) => mask.get.faultToleranceTests = mask.get.faultToleranceTests :+ tolerance
              case None =>
            }
          }
        }
      case None =>
    }
    result.toMap
  }

  def getSourceByFilename(sources: Map[String, Source], filename: String): Option[Source] = {
    val fragments = filename.split("/")
    sources.get(fragments(1))
  }

  def getMaskByFilename(source: Source, filename: String): Option[Mask] = {

    for (mask <- source.masks) {
      if (mask.active && filename.matches(mask.mask)) return Some(mask)
    }
    None
  }

  override def getImportItemsByUseCase(useCase: String): Seq[ImportItem] = {
    val items = CoreRepositories.hiveRepository.sql(IngestConstants.HqlMetadataGetUseCaseItems, useCase)
    var result = Seq[ImportItem]()

    items match {
      case Some(importItems) =>
        importItems.collect.foreach { row =>
          val codItem = row.getAs[Int]("cod_item")
          val codUsecase = row.getAs[Int]("cod_usecase")
          val source = row.getAs[String]("source")
          val sourceSchema = row.getAs[String]("source_schema")
          val sourceTable = row.getAs[String]("source_table")
          val targetSchema = row.getAs[String]("target_schema")
          val targetTable = row.getAs[String]("target_table")
          val targetDir = row.getAs[String]("target_dir")
          val partitionType = PartitionTypes.getObject(row.getAs[String]("partition_type"))
          val fieldDelimiter = pipeDelimeter(row.getAs[String]("field_delimiter"))
          val fieldDelimiterHex = row.getAs[String]("field_delimiter_hex")
          val lineDelimiter = row.getAs[String]("line_delimiter")
          val lineDelimiterHex = row.getAs[String]("line_delimiter_hex")
          val fileFormat = FileFormats.getObject(row.getAs[String]("format"))
          val compressionCodec = CompressionCodecs.getObject(row.getAs[String]("compression_codec"))
          val generateFlag = row.getAs[Boolean]("generate_flag")
          val importToPreraw = row.getAs[Boolean]("import_to_preraw")
          val enclosedByQuotes = row.getAs[Boolean]("enclosed_by_quotes")
          val escapedByBackslash = row.getAs[Boolean]("escaped_by_backslash")

          result = result.+:(ImportItem(codItem, codUsecase, source, sourceSchema, sourceTable, targetSchema,
            targetTable, targetDir, partitionType, fieldDelimiter, fieldDelimiterHex,
            lineDelimiter, lineDelimiterHex, fileFormat, compressionCodec, generateFlag, importToPreraw, enclosedByQuotes,
            escapedByBackslash))
        }
      case None => CoreContext.logger.info(s"Items para usecase $useCase no encontrados")
    }

    result
  }

  def filterFilesByMask(filenames: Seq[String], mask: Mask): Seq[String] = {

    var result = Seq[String]()
    for (filename <- filenames) {
      val fragments = filename.split("/")
      val file = fragments.drop(5).mkString("/")
      if (file.matches(mask.mask)) result = result :+ filename
    }
    result
  }
  
  def pipeDelimeter(delimiter: String): String = {
    if(delimiter == "|") {
      val result = "\\|"
      result
    }
    else{
      delimiter
    }
  }

}
