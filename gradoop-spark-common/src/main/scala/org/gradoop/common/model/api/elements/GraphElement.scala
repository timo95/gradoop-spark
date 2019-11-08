package org.gradoop.common.model.api.elements

trait GraphElement extends Element {
  def graphIds: IdSet
  def graphCount: Int = graphIds.size

  def graphIds_=(graphIds: IdSet): Unit
}
