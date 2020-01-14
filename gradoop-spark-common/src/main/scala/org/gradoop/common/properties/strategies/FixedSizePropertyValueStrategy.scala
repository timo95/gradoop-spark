package org.gradoop.common.properties.strategies

import org.gradoop.common.properties.PropertyValue

trait FixedSizePropertyValueStrategy[A] extends PropertyValueStrategy[A] {

  override def getBytes(value: A): Array[Byte] = {
    val bytes = new Array[Byte](getSize(getRawSize(value)))
    putBytes(bytes, 0, value)
    bytes
  }

  def getRawSize: Int

  override def getRawSize(value: A): Int = getRawSize

  override def getSize(rawSize: Int): Int = rawSize + PropertyValue.OFFSET
}
