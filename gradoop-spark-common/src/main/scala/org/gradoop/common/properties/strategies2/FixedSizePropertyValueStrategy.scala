package org.gradoop.common.properties.strategies2

import org.gradoop.common.properties.PropertyValue

trait FixedSizePropertyValueStrategy[A] extends PropertyValueStrategy[A] {

  def getRawSize: Int

  override def getRawSize(value: A): Int = getRawSize

  override def getSize(value: A): Int = getRawSize + PropertyValue.OFFSET
}
