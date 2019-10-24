package org.gradoop.common.model.api.elements

trait GraphElement extends Element {
  def getGraphIds: IdSet
  def getGraphCount: Int = getGraphIds.size
}
