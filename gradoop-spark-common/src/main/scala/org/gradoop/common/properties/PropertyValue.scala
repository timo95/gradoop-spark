package org.gradoop.common.properties

case class PropertyValue(value: Array[Byte], typeByte: Byte) {

  def getType: Type = Type(typeByte) // TODO typeByte does not have compound types

  def getString: String = new String(value)

  def get: Any = getString

  override def toString: Label = get.toString
}

object PropertyValue {

  val NULL_VALUE = new PropertyValue(Array.empty, Type.NULL.byte)

  def apply(value: Any): PropertyValue = {
    if(value == null) PropertyValue("")
    else PropertyValue(value.toString)
  } // TODO to byte for supported types

  def apply(value: Int): PropertyValue = new PropertyValue(Array(value.toByte), Type.INTEGER.byte)
  def apply(value: Double): PropertyValue = new PropertyValue(Array(value.toByte), Type.DOUBLE.byte)
  def apply(value: String): PropertyValue = new PropertyValue(value.getBytes, Type.STRING.byte)
}
