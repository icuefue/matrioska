package com.bluetab.matrioska.core.exceptions

/**
  * Formato de fichero de importación no soportado por Sqoop
  *
  * Created by xe54068 on 01/02/2017.
  */
class NotSupportedSqoopFileFormat(e: String) extends Exception(e)