package org.gradoop.common.model.api.elements

trait Edge extends GraphElement {
  def getSourceId: Id
  def getTargetId: Id
}
