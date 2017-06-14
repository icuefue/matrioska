package com.bbva.ebdm.linx.ingest.beans

import com.bbva.ebdm.linx.core.exceptions.FatalException

case class FileTypeBean(fileType: FileTypeEnum.Value, fieldDelimiter: String, lineDelimiter: String,
                        endsWithDelimiter: Boolean, header: Int, dateFormat: String, linePattern: String,
                        enclosedBy: String, escapedBy: String)

object FileTypeFactory {

  def apply(fileTypeBean: FileTypeBean) = fileTypeBean.fileType match {
    case FileTypeEnum.DelimitedFile => new DelimitedFileType(fileTypeBean.fieldDelimiter, fileTypeBean.lineDelimiter,
      fileTypeBean.endsWithDelimiter, fileTypeBean.header, fileTypeBean.dateFormat)
    case FileTypeEnum.SqoopCsvFile => new SqoopCsvFileType(fileTypeBean.fieldDelimiter, fileTypeBean.lineDelimiter,
      fileTypeBean.enclosedBy, fileTypeBean.escapedBy, fileTypeBean.header, fileTypeBean.dateFormat)
    case FileTypeEnum.FixedLengthFile => new FixedLengthFileType(fileTypeBean.linePattern, fileTypeBean.header,
      fileTypeBean.dateFormat)
    case FileTypeEnum.AllInOneFieldFile => new AllInOneFieldFileType(fileTypeBean.dateFormat)
    case _ => {
      throw new FatalException("Tipo de fichero no existe")
    }
  }

}
