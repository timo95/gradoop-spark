package org.gradoop.common.model.api.elements

trait GraphMember {
  def graphIds: IdSet
  def graphCount: Int = graphIds.size

  def graphIds_=(graphIds: IdSet): Unit
}
