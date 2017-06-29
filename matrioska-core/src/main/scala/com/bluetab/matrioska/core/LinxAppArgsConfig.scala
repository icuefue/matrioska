package com.bluetab.matrioska.core

/**
  * Configuraciones para una app submit
  *
  * @param appName - App que se va a ejecutar
  * @param appArgs - argumentos opcionales de la app que se va a ejecutar
  * @param planDate - fecha de planificación del proceso
  * @param force - flag opcional para poder ejecutar el proceso sin que esté dado de alta en la tabla de planificaciones
  */
case class LinxAppArgsConfig(appName: String = "", appArgs: Seq[String] = Seq(), planDate : String ="", force : Boolean = false, isPro : Boolean = false, logLevel : String = "DEBUG")
