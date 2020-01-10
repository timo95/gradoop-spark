package org.gradoop.common.properties.strategies2

import org.gradoop.common.properties.Type
import org.gradoop.common.properties.bytes.Bytes

object BigDecimalStrategy extends VariableSizedPropertyValueStrategy[BigDecimal] {

  override def putRawBytes(bytes: Array[Byte], offset: Int, value: BigDecimal): Unit = {
    val valueBytes = Bytes.toBytes(value.bigDecimal)
    Bytes.putBytes(bytes, offset, valueBytes, 0, valueBytes.length)
  }

  override def fromRawBytes(bytes: Array[Byte], offset: Int, size: Int): BigDecimal = {
    Bytes.toBigDecimal(bytes, offset, size)
  }

  override def compare(value: BigDecimal, other: Any): Int = {
    other match {
      case num: Number => PropertyValueStrategyUtils.compareNumerical(value, num)
      case _ => throw new IllegalArgumentException("Incompatible types: %s, %s".format(value.getClass, other.getClass))
    }  }

  override def is(value: Any): Boolean = value.isInstanceOf[BigDecimal]

  override def getRawSize(value: BigDecimal): Int = {
    value.bigDecimal.unscaledValue.toByteArray.length + Bytes.SIZEOF_INT
  }

  override def getType: Type = Type.BIG_DECIMAL

  override def getExactType(value: BigDecimal): Type = getType
}
