package org.gradoop.spark.model.api.elements

trait GraphElement extends Element {
  def getGraphIds: IdSet

}
