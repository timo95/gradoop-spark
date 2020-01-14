package org.gradoop.common.properties.strategies

import org.gradoop.common.properties.bytes.Bytes
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.Type

object LongStrategy extends FixedSizePropertyValueStrategy[Long] {

  override def putBytes(bytes: Array[Byte], offset: Int, value: Long): Unit = {
    Bytes.putByte(bytes, offset, getType.byte)
    Bytes.putLong(bytes, offset + PropertyValue.OFFSET, value)
  }

  override def fromBytes(bytes: Array[Byte], offset: Int): Long = {
    Bytes.toLong(bytes, offset + PropertyValue.OFFSET)
  }

  override def compare(value: Long, other: Any): Int = {
    PropertyValueStrategyUtils.compareNumerical(value, other)
  }

  override def is(value: Any): Boolean = value.isInstanceOf[Long]

  override def getRawSize: Int = Bytes.SIZEOF_LONG

  override def getType: Type = Type.LONG
}
