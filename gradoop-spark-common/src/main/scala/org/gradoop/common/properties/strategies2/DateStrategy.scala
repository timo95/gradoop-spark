package org.gradoop.common.properties.strategies2

import java.time.LocalDate

import org.gradoop.common.properties.bytes.Bytes
import org.gradoop.common.properties.{DateTimeSerializer, PropertyValue, Type}

object DateStrategy extends FixedSizePropertyValueStrategy[LocalDate] {

  override def putBytes(bytes: Array[Byte], offset: Int, value: LocalDate): Unit = {
    Bytes.putByte(bytes, offset, getType.byte)
    val valueBytes = DateTimeSerializer.serializeDate(value)
    Bytes.putBytes(bytes, offset + PropertyValue.OFFSET, valueBytes, 0, valueBytes.length)
  }

  override def fromBytes(bytes: Array[Byte], offset: Int): LocalDate = {
    DateTimeSerializer.deserializeDate(bytes, offset + PropertyValue.OFFSET)
  }

  override def compare(value: LocalDate, other: Any): Int = {
    other match {
      case date: LocalDate => value.compareTo(date)
      case _ => throw new IllegalArgumentException(String.format("Incompatible types: %s, %s", value.getClass, other.getClass))
    }
  }

  override def is(value: Any): Boolean = value.isInstanceOf[LocalDate]

  override def getRawSize: Int = DateTimeSerializer.SIZEOF_DATE

  override def getType: Type = Type.DATE
}
