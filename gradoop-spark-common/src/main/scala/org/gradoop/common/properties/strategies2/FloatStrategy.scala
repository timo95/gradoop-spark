package org.gradoop.common.properties.strategies2
import org.gradoop.common.properties.{PropertyValue, Type}
import org.gradoop.common.properties.bytes.Bytes

object FloatStrategy extends FixedSizePropertyValueStrategy[Float] {

  override def putBytes(bytes: Array[Byte], offset: Int, value: Float): Unit = {
    Bytes.putByte(bytes, offset, getType.byte)
    Bytes.putFloat(bytes, offset + PropertyValue.OFFSET, value)
  }

  override def fromBytes(bytes: Array[Byte], offset: Int): Float = {
    Bytes.toFloat(bytes, offset + PropertyValue.OFFSET)
  }

  override def compare(value: Float, other: Any): Int = {
    PropertyValueStrategyUtils.compareNumerical(value, other)
  }

  override def is(value: Any): Boolean = value.isInstanceOf[Float]

  override def getRawSize: Int = Bytes.SIZEOF_FLOAT

  override def getType: Type = Type.FLOAT
}
