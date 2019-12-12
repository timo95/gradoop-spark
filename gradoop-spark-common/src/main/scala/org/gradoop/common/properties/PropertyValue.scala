package org.gradoop.common.properties

import org.gradoop.common.properties.bytes.Bytes

case class PropertyValue(value: Array[Byte], typeByte: Byte) {

  def getType: Type = Type(typeByte) // TODO typeByte does not have compound types

  def getString: String = new String(value)

  def getBoolean: Boolean = value(0) == -1;
  def getInt: Int = Bytes.toInt(value)

  def get: Any = {
    typeByte match {
      case Type.BOOLEAN.byte => getBoolean
      case Type.INTEGER.byte => getInt
      case _ => getString
    }
  }

  override def toString: Label = s"${get.toString}:${Type(typeByte).string}"
}

object PropertyValue {

  val NULL_VALUE = new PropertyValue(Array.empty, Type.NULL.byte)

  def apply(value: Any): PropertyValue = {
    value match {
      case null => apply("")
      case b: Boolean => apply(b)
      case i: Int => apply(i)
      case _ => PropertyValue(value.toString)
    }
  } // TODO to byte for supported types

  def apply(value: Boolean): PropertyValue = {
    new PropertyValue(Array((if (value) -1 else 0).toByte), Type.BOOLEAN.byte)
  }

  def apply(value: Int): PropertyValue = {
    val rawBytes = new Array[Byte](Bytes.SIZEOF_INT)
    Bytes.putInt(rawBytes, 0, value)
    new PropertyValue(rawBytes, Type.INTEGER.byte)
  }

  def apply(value: Double): PropertyValue = new PropertyValue(Array(value.toByte), Type.DOUBLE.byte)

  def apply(value: String): PropertyValue = new PropertyValue(value.getBytes, Type.STRING.byte)
}
