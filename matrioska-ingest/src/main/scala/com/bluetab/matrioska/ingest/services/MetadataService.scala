package com.bluetab.matrioska.ingest.services

import com.bluetab.matrioska.ingest.beans.Mask
import com.bluetab.matrioska.ingest.beans.Source
import com.bluetab.matrioska.ingest.beans.ImportItem

/**
 * Servicio de importación de tablas de parametría
 */
trait MetadataService {

  def getStagingPaths: Seq[String]
  def loadSources: Map[String, Source]

  def getSourceByFilename(sources: Map[String, Source], filename: String): Option[Source]

  def getMaskByFilename(source: Source, filename: String): Option[Mask]

  def filterFilesByMask(filenames: Seq[String], mask: Mask): Seq[String]

  /**
   * Obtención de los ImportItems de una determinado caso de uso
   *
   * @param useCase - nombre del caso de uso
   * @return - Seq con los ImportItems
   */
  def getImportItemsByUseCase(useCase: String): Seq[ImportItem]
}
