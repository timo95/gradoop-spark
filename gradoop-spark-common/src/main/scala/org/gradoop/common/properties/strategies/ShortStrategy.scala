package org.gradoop.common.properties.strategies

import org.gradoop.common.properties.bytes.Bytes
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.Type

object ShortStrategy extends FixedSizePropertyValueStrategy[Short] {

  override def putBytes(bytes: Array[Byte], offset: Int, value: Short): Unit = {
    Bytes.putByte(bytes, offset, getType.byte)
    Bytes.putShort(bytes, offset + PropertyValue.OFFSET, value)
  }

  override def fromBytes(bytes: Array[Byte], offset: Int): Short = {
    Bytes.toShort(bytes, offset + PropertyValue.OFFSET)
  }

  override def compare(value: Short, other: Any): Int = {
    PropertyValueStrategyUtils.compareNumerical(value, other)
  }

  override def is(value: Any): Boolean = value.isInstanceOf[Short]

  override def getRawSize: Int = Bytes.SIZEOF_SHORT

  override def getType: Type = Type.SHORT
}
