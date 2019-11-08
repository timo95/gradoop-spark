package org.gradoop.common.model.api.elements

trait Attributed extends Serializable {
  def properties: Properties

  def properties_=(properties: Properties): Unit
}
