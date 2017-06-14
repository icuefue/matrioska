package com.bbva.ebdm.linx.core.beans

case class UseCase(id: String, items: Seq[Item])

case class Item(mask: String, destination: Destination)

case class Destination(destinationType: String, destination: String)
