package org.gradoop.common.model.api.components

trait GraphElement {

  def graphIds: IdSet

  def graphCount: Int = graphIds.size

  def graphIds_=(graphIds: IdSet): Unit
}
