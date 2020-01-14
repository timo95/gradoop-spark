package org.gradoop.common.properties.strategies

import org.gradoop.common.properties.bytes.Bytes
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.Type

object DoubleStrategy extends FixedSizePropertyValueStrategy[Double] {

  override def putBytes(bytes: Array[Byte], offset: Int, value: Double): Unit = {
    Bytes.putByte(bytes, offset, getType.byte)
    Bytes.putDouble(bytes, offset + PropertyValue.OFFSET, value)
  }

  override def fromBytes(bytes: Array[Byte], offset: Int): Double = {
    Bytes.toDouble(bytes, offset + PropertyValue.OFFSET)
  }

  override def compare(value: Double, other: Any): Int = {
    PropertyValueStrategyUtils.compareNumerical(value, other)
  }

  override def is(value: Any): Boolean = value.isInstanceOf[Double]

  override def getRawSize: Int = Bytes.SIZEOF_DOUBLE

  override def getType: Type = Type.DOUBLE
}
