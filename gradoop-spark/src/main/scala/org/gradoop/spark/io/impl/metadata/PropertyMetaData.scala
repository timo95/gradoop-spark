package org.gradoop.spark.io.impl.metadata

import org.gradoop.common.util.Type

case class PropertyMetaData(key: String, typeString: String)

object PropertyMetaData {
  val key = "key"
  val typeString = "typeString"

  def apply(key: String, propertyType: Type): PropertyMetaData = PropertyMetaData(key, propertyType.string)
}
