package org.gradoop.common.properties

case class PropertyValue(value: Array[Byte], typeByte: Byte) extends Serializable {

  def getType: Type = Type(typeByte)

  def getString: String = new String(value)

  def get: Any = getString

  override def toString: Label = get.toString
}

object PropertyValue {

  val NULL_VALUE = new PropertyValue(Array.empty, Type.Null.byte)

  def apply(value: Any): PropertyValue = {
    if(value == null) PropertyValue("")
    else PropertyValue(value.toString)
  } // TODO to byte for supported types

  def apply(value: Int): PropertyValue = new PropertyValue(Array(value.toByte), Type.Integer.byte)
  def apply(value: Double): PropertyValue = new PropertyValue(Array(value.toByte), Type.Double.byte)
  def apply(value: String): PropertyValue = new PropertyValue(value.getBytes, Type.String.byte)
}
