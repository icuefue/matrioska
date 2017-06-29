package com.bluetab.matrioska.ingest.services.impl

import java.io.File
import java.nio.charset.Charset
import java.nio.file.Paths

import com.bluetab.matrioska.core.conf.{CoreConfig, CoreRepositories}
import com.bluetab.matrioska.core.constants.CoreConstants
import com.bluetab.matrioska.ingest.constants.{IngestConstants, IngestDfsConstants}
import com.bluetab.matrioska.ingest.services.StagingToPrerawService
import org.joda.time.DateTime

object StagingToPrerawConstants {
  val Year_Field = "year="
  val Month_Field = "month="
  val Day_Field = "day="
  val preRawPathToLoad = IngestDfsConstants.PrerawToLoad
  val sufixINPROGRESS = IngestConstants.SufixProcesando
  val sufixPROCESADO = IngestConstants.SufixProcesado
}

class StagingToPrerawServiceImpl extends StagingToPrerawService {

  def getToLoadFilenames: Seq[File] = {
    val fileStaginPath = new File(CoreConfig.localfilesystempath.staging)
    val filenames = CoreRepositories.fsRepository.listFiles(fileStaginPath, true: Boolean, true: Boolean)
    filenames
  }

  def enableToStart(file: String): Boolean = {
    if (!file.contains(StagingToPrerawConstants.sufixINPROGRESS) && !file.contains(StagingToPrerawConstants.sufixPROCESADO)) {
      true
    } else {
      false
    }
  }

  def prepareFile(file: File): File = {
    val fileSufix = CoreRepositories.fsRepository.renameFile(file, StagingToPrerawConstants.sufixINPROGRESS)
    fileSufix
  }

  def copyFileToHDFS(pathFile: File, dateCreation: DateTime) {

    var sourcePathFile = pathFile.toString
    val pathFileNameWithoutSufix = sourcePathFile.dropRight(StagingToPrerawConstants.sufixINPROGRESS.length)
    val sourcePath = Paths.get(pathFileNameWithoutSufix)
    val directory = sourcePath.getParent.toString
    val fileName = pathFileNameWithoutSufix.drop(directory.length)
    val year = dateCreation.getYear
    val month = dateCreation.getMonthOfYear
    val day = dateCreation.getDayOfMonth

    val originAbsolute = sourcePathFile.drop(CoreConfig.localfilesystempath.staging.length)
    val origin = originAbsolute.split(CoreConstants.slash)(1)
    val originWithoutFile = originAbsolute.dropRight(fileName.length + StagingToPrerawConstants.sufixINPROGRESS.length)
    val subFolders = originWithoutFile.drop(origin.length + 1) // El +1 es para añadir el caracter de la barra(/)

    val destinationPath = StagingToPrerawConstants.preRawPathToLoad +
      CoreConstants.slash + origin +
      CoreConstants.slash + StagingToPrerawConstants.Year_Field + year +
      CoreConstants.slash + StagingToPrerawConstants.Month_Field + month +
      CoreConstants.slash + StagingToPrerawConstants.Day_Field + day +
      subFolders

    if (!CoreRepositories.dfsRepository.exists(destinationPath)) {
      CoreRepositories.dfsRepository.mkdirs(destinationPath)
    }
    val destinationPathFile = destinationPath + fileName
    val charset = CoreRepositories.fsRepository.detectFileCharset(sourcePathFile)
    var convertToUtf8 = false
    print("\n\n" + charset.name() + "\n\n")
    charset.name() match {
      case "windows-1252" => convertToUtf8 = true
      case default =>
    }

    if (convertToUtf8) {
      val sourcePathFileTemp = sourcePathFile + ".temp"
      CoreRepositories.fsRepository.convertFile(sourcePathFile, sourcePathFileTemp, charset, Charset.forName("UTF-8"))
      CoreRepositories.dfsRepository.copyFromLocalToHDFS(sourcePathFileTemp, destinationPathFile)
      CoreRepositories.fsRepository.deleteFile(sourcePathFile)
    } else {
      CoreRepositories.dfsRepository.copyFromLocalToHDFS(sourcePathFile, destinationPathFile)
    }

  }

  def movedSuccessfully(file: File) {
    val originalFileName = CoreRepositories.fsRepository.deleteSufix(file, StagingToPrerawConstants.sufixINPROGRESS)
    CoreRepositories.fsRepository.renameFile(originalFileName, StagingToPrerawConstants.sufixPROCESADO)
  }

  def deleteFileSufix(file: File) {
    CoreRepositories.fsRepository.deleteSufix(file, StagingToPrerawConstants.sufixINPROGRESS)
  }

}