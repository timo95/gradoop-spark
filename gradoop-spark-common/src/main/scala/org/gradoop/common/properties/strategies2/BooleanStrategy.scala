package org.gradoop.common.properties.strategies2

import org.gradoop.common.properties.{PropertyValue, Type}
import org.gradoop.common.properties.bytes.Bytes

object BooleanStrategy extends FixedSizePropertyValueStrategy[Boolean] {

  override def putBytes(bytes: Array[Byte], offset: Int, value: Boolean): Unit = {
    Bytes.putByte(bytes, offset, getType.byte)
    val byte: Byte = if(value) -1 else 0
    Bytes.putByte(bytes, offset + PropertyValue.OFFSET, byte)
  }

  override def fromBytes(bytes: Array[Byte], offset: Int): Boolean = {
    bytes(offset + PropertyValue.OFFSET) == -1
  }

  override def compare(value: Boolean, other: Any): Int = {
    other match {
      case bool: Boolean => value.compare(bool)
      case _ => throw new IllegalArgumentException(String.format("Incompatible types: %s, %s", value.getClass, other.getClass))
    }
  }

  override def is(value: Any): Boolean = value.isInstanceOf[Boolean]

  override def getRawSize: Int = Bytes.SIZEOF_BOOLEAN

  override def getType: Type = Type.BOOLEAN
}
