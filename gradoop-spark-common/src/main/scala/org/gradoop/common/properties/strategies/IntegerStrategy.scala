package org.gradoop.common.properties.strategies

import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.properties.bytes.Bytes
import org.gradoop.common.util.Type

object IntegerStrategy extends FixedSizePropertyValueStrategy[Int] {

  override def putBytes(bytes: Array[Byte], offset: Int, value: Int): Unit = {
    Bytes.putByte(bytes, offset, getType.byte)
    Bytes.putInt(bytes, offset + PropertyValue.OFFSET, value)
  }

  override def fromBytes(bytes: Array[Byte], offset: Int): Int = {
    Bytes.toInt(bytes, offset + PropertyValue.OFFSET)
  }

  override def compare(value: Int, other: Any): Int = {
    PropertyValueStrategyUtils.compareNumerical(value, other)
  }

  override def is(value: Any): Boolean = value.isInstanceOf[Int]

  override def getRawSize: Int = Bytes.SIZEOF_INT

  override def getType: Type = Type.INTEGER
}
