package org.gradoop.common.model.api.elements

trait Attributed {
  def getProperties: Properties

  def setProperties(properties: Properties): Unit
}
