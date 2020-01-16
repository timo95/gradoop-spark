package org.gradoop.common.properties.strategies

import java.util

import org.gradoop.common.id.GradoopId
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.properties.bytes.Bytes
import org.gradoop.common.util.Type

object GradoopIdStrategy extends FixedSizePropertyValueStrategy[GradoopId] {

  override def putBytes(bytes: Array[Byte], offset: Int, value: GradoopId): Unit = {
    Bytes.putByte(bytes, offset, getType.byte)
    Bytes.putBytes(bytes, offset + PropertyValue.OFFSET, value.bytes, 0, value.bytes.length)
  }

  override def fromBytes(bytes: Array[Byte], offset: Int): GradoopId = {
    val value = bytes.slice(offset + PropertyValue.OFFSET, offset + PropertyValue.OFFSET + getRawSize)
    GradoopId(value)
  }

  override def compare(value: GradoopId, other: Any): Int = {
    other match {
      case id: GradoopId => value.compareTo(id)
      case _ => throw new IllegalArgumentException(String.format("Incompatible types: %s, %s", value.getClass, other.getClass))
    }
  }

  override def is(value: Any): Boolean = value.isInstanceOf[GradoopId]

  override def getRawSize: Int = GradoopId.ID_SIZE

  override def getType: Type = Type.GRADOOP_ID
}
