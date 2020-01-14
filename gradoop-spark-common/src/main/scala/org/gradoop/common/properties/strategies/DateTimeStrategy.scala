package org.gradoop.common.properties.strategies

import java.time.LocalDateTime

import org.gradoop.common.properties.bytes.{Bytes, DateTimeSerializer}
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.Type

object DateTimeStrategy extends FixedSizePropertyValueStrategy[LocalDateTime] {

  override def putBytes(bytes: Array[Byte], offset: Int, value: LocalDateTime): Unit = {
    Bytes.putByte(bytes, offset, getType.byte)
    val valueBytes = DateTimeSerializer.serializeDateTime(value)
    Bytes.putBytes(bytes, offset + PropertyValue.OFFSET, valueBytes, 0, valueBytes.length)
  }

  override def fromBytes(bytes: Array[Byte], offset: Int): LocalDateTime = {
    DateTimeSerializer.deserializeDateTime(bytes, offset + PropertyValue.OFFSET)
  }

  override def compare(value: LocalDateTime, other: Any): Int = {
    other match {
      case date: LocalDateTime => value.compareTo(date)
      case _ => throw new IllegalArgumentException(String.format("Incompatible types: %s, %s", value.getClass, other.getClass))
    }
  }

  override def is(value: Any): Boolean = value.isInstanceOf[LocalDateTime]

  override def getRawSize: Int = DateTimeSerializer.SIZEOF_DATETIME

  override def getType: Type = Type.DATE_TIME
}
