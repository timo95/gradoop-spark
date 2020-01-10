package org.gradoop.common.properties.strategies2

import org.gradoop.common.properties.Type
import org.gradoop.common.properties.bytes.Bytes

object NullStrategy extends FixedSizePropertyValueStrategy[Null] {

  override def putBytes(bytes: Array[Byte], offset: Int, value: Null): Unit = {
    Bytes.putByte(bytes, offset, getType.byte)
  }

  override def fromBytes(bytes: Array[Byte], offset: Int): Null = null

  override def compare(value: Null, other: Any): Int = {
    if (other == null) 0
    else -1
  }

  override def is(value: Any): Boolean = value == null

  override def getRawSize: Int = 0

  override def getType: Type = Type.NULL
}
