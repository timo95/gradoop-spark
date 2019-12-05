package org.gradoop.common.model.api.components

import org.gradoop.common.properties.PropertyValue

trait Property {

  def key: String

  def value: PropertyValue
}
