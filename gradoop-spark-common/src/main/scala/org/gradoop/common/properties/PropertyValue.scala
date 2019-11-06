package org.gradoop.common.properties

class PropertyValue(value: Array[Byte]) extends Serializable {

  def getString: String = {
    new String(value)
  }
}

object PropertyValue {

  def apply(value: String): PropertyValue = {
    new PropertyValue(value.getBytes)
  }
}
