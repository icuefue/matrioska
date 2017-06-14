package com.bluetab.matrioska.core.beans

case class UseCase(id: String, items: Seq[Item])

case class Item(mask: String, destination: Destination)

case class Destination(destinationType: String, destination: String)
