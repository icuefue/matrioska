package com.bbva.ebdm.linx.core

/**
 * Argumentos de una app submit
 */
object LinxAppArgs {

  var appName: String = ""
  var appArgs: Seq[String] = Seq()
  var planDate: String = ""
  var force: Boolean = false
  var isApp: Boolean = false
  var isPro: Boolean = false
  var user: String = ""
  var topic: String = ""
  var group: String = ""
  var interval: Int = 300
  var logLevel: String = ""
}
