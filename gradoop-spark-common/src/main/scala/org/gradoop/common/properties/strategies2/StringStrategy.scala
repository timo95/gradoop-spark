package org.gradoop.common.properties.strategies2

import java.nio.charset.StandardCharsets

import org.gradoop.common.properties.Type
import org.gradoop.common.properties.bytes.Bytes

object StringStrategy extends VariableSizedPropertyValueStrategy[String] {

  override def putRawBytes(bytes: Array[Byte], offset: Int, value: String): Unit = {
    val valueBytes = Bytes.toBytes(value)
    Bytes.putBytes(bytes, offset, valueBytes, 0, valueBytes.length)
  }

  override def fromRawBytes(bytes: Array[Byte], offset: Int, size: Int): String = {
    Bytes.toString(bytes, offset, size)
  }

  override def compare(value: String, other: Any): Int = {
    other match {
      case str: String => value.compareTo(str)
      case _ => throw new IllegalArgumentException(String.format("Incompatible types: %s, %s", value.getClass, other.getClass))
    }
  }

  override def is(value: Any): Boolean = value.isInstanceOf[String]

  override def getRawSize(value: String): Int = value.getBytes(StandardCharsets.UTF_8).length;

  override def getType: Type = Type.STRING

  override def getExactType(value: String): Type = getType
}
