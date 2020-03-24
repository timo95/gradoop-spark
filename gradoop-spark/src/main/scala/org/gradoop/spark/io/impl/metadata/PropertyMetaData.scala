package org.gradoop.spark.io.impl.metadata

import org.gradoop.common.util.Type

case class PropertyMetaData(key: String, typeString: String)

object PropertyMetaData {
  // Field names
  val key = "key"
  val typeString = "typeString"

  def apply(key: String, typeString: String): PropertyMetaData = new PropertyMetaData(key, typeString)

  def apply(key: String, propertyType: Type): PropertyMetaData = new PropertyMetaData(key, propertyType.string)
}
