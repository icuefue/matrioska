package com.bbva.ebdm.linx.ingest.beans

class AuditFileData extends Serializable{
  var path: String = ""
  var totalLines: Int = 0
  var okLines: Int = 0
  var errors: Seq[String] = Seq[String]()
}
