package org.gradoop.common.properties.strategies2

import java.time.LocalTime

import org.gradoop.common.properties.bytes.Bytes
import org.gradoop.common.properties.{DateTimeSerializer, PropertyValue, Type}

object TimeStrategy extends FixedSizePropertyValueStrategy[LocalTime] {

  override def putBytes(bytes: Array[Byte], offset: Int, value: LocalTime): Unit = {
    Bytes.putByte(bytes, offset, getType.byte)
    val valueBytes = DateTimeSerializer.serializeTime(value)
    Bytes.putBytes(bytes, offset + PropertyValue.OFFSET, valueBytes, 0, valueBytes.length)
  }

  override def fromBytes(bytes: Array[Byte], offset: Int): LocalTime = {
    DateTimeSerializer.deserializeTime(bytes, offset + PropertyValue.OFFSET)
  }

  override def compare(value: LocalTime, other: Any): Int = {
    other match {
      case date: LocalTime => value.compareTo(date)
      case _ => throw new IllegalArgumentException(String.format("Incompatible types: %s, %s", value.getClass, other.getClass))
    }
  }

  override def is(value: Any): Boolean = value.isInstanceOf[LocalTime]

  override def getRawSize: Int = DateTimeSerializer.SIZEOF_TIME

  override def getType: Type = Type.TIME
}
