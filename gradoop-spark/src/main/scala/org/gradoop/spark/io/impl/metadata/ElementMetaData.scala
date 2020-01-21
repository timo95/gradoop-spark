package org.gradoop.spark.io.impl.metadata

case class ElementMetaData(label: String, metaData: Seq[PropertyMetaData])

object ElementMetaData {
  val label = "label"
  val metaData = "metaData"
}