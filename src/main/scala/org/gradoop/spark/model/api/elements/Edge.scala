package org.gradoop.spark.model.api.elements

trait Edge extends GraphElement {
  def getSourceId: Id
  def getTargetId: Id
}
