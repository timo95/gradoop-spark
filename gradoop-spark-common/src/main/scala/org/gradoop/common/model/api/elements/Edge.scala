package org.gradoop.common.model.api.elements

trait Edge extends GraphElement {
  def sourceId: Id

  def targetId: Id

  def sourceId_=(sourceId: Id): Unit

  def targetId_=(targetId: Id): Unit
}
