package com.bbva.ebdm.linx.core.beans

import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat

case class Event(headers: Header, body: String) {
  def getPartitionDate: String = {
    val formattedDate = DateTime.parse(headers.date)
    DateTimeFormat.forPattern("yyyyMMdd").print(formattedDate)
  }
}

case class Header(basename: String, use_case: String, date: String)
