package com.bbva.ebdm.linx.core

/**
  * Configuraciones para una app straming
  *
  * @param appName - App que se va a ejecutar
  * @param appArgs - argumentos opcionales de la app que se va a ejecutar
  */
case class LinxStreamingAppArgsConfig(appName: String = "", appArgs: Seq[String] = Seq(), 
    topic: String = "", group: String = "", interval: Int = 300, isPro: Boolean = false, logLevel : String = "DEBUG")
