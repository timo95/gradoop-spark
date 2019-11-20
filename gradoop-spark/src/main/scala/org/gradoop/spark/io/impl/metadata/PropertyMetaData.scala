package org.gradoop.spark.io.impl.metadata

import org.gradoop.common.properties.Type

case class PropertyMetaData(key: String, typeByte: Byte)

object PropertyMetaData {
  def apply(key: String, typeString: String): PropertyMetaData = PropertyMetaData(key, Type(typeString).byte)
}