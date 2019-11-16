package org.gradoop.common.model.api.elements

trait EdgeType {
  def sourceId: Id

  def targetId: Id

  def sourceId_=(sourceId: Id): Unit

  def targetId_=(targetId: Id): Unit
}
