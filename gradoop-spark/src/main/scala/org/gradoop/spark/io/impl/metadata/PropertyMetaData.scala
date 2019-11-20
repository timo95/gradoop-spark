package org.gradoop.spark.io.impl.metadata

import org.gradoop.common.properties.Type

case class PropertyMetaData(key: String, typeString: String)

object PropertyMetaData {
  def apply(key: String, propertyType: Type): PropertyMetaData = PropertyMetaData(key, propertyType.string)
}