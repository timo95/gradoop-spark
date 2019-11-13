package org.gradoop.common.properties

case class PropertyValue(value: Array[Byte], typeByte: Byte) extends Serializable {

  def getString: String = new String(value)

  def get: Any = getString

  override def toString: Labels = get.toString
}

object PropertyValue {
  val TYPE_STRING: Byte = 0
  val TYPE_INT: Byte = 1
  val TYPE_DOUBLE: Byte = 2

  def apply(value: String): PropertyValue = new PropertyValue(value.getBytes, TYPE_STRING)

  def apply(value: Int): PropertyValue = new PropertyValue(Array(value.toByte), TYPE_INT)

  def apply(value: Double): PropertyValue = new PropertyValue(Array(value.toByte), TYPE_DOUBLE)
}
