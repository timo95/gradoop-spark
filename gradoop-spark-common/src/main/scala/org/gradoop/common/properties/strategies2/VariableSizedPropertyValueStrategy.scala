package org.gradoop.common.properties.strategies2

import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.properties.bytes.Bytes

trait VariableSizedPropertyValueStrategy[A] extends PropertyValueStrategy[A] {

  override def putBytes(bytes: Array[Byte], offset: Int, value: A): Unit = {
    val size = getRawSize(value)
    if(size > PropertyValue.LARGE_PROPERTY_THRESHOLD) {
      Bytes.putByte(bytes, offset, (getType.byte | PropertyValue.FLAG_LARGE).asInstanceOf[Byte])
      Bytes.putInt(bytes, offset + PropertyValue.OFFSET, size)
      putRawBytes(bytes, offset + PropertyValue.OFFSET + Bytes.SIZEOF_INT, value)
    } else {
      Bytes.putByte(bytes, offset, getType.byte)
      Bytes.putShort(bytes, offset + PropertyValue.OFFSET, size.asInstanceOf[Short])
      putRawBytes(bytes, offset + PropertyValue.OFFSET + Bytes.SIZEOF_SHORT, value)
    }
  }

  def putRawBytes(bytes: Array[Byte], offset: Int, value: A): Unit

  def fromRawBytes(bytes: Array[Byte], offset: Int, size: Int): A

  override def fromBytes(bytes: Array[Byte], offset: Int): A = {
    if((bytes(offset) & PropertyValue.FLAG_LARGE) == PropertyValue.FLAG_LARGE) {
      val size = Bytes.toInt(bytes, offset + PropertyValue.OFFSET)
      fromRawBytes(bytes, offset + PropertyValue.OFFSET + Bytes.SIZEOF_INT, size)
    } else {
      val size = Bytes.toShort(bytes, offset + PropertyValue.OFFSET)
      fromRawBytes(bytes, offset + PropertyValue.OFFSET + Bytes.SIZEOF_SHORT, size)
    }
  }

  override def getSize(value: A): Int = {
    val size = getRawSize(value)
    if(size > PropertyValue.LARGE_PROPERTY_THRESHOLD) size + PropertyValue.OFFSET + Bytes.SIZEOF_INT
    else size + PropertyValue.OFFSET + Bytes.SIZEOF_SHORT
  }
}
