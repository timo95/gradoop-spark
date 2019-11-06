package org.gradoop.common.model.api.elements

trait Attributed extends Serializable {
  def getProperties: Properties

  def setProperties(properties: Properties): Unit
}
