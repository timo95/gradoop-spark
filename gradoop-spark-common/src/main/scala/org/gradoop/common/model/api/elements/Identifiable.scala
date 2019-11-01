package org.gradoop.common.model.api.elements

import org.gradoop.common.model.impl.id.GradoopId

trait Identifiable {

  /** Returns the identifier of that entity.
   *
   * @return identifier
   */
  def getId: Id

  /** Sets the identifier of that entity.
   *
   * @param id identifier
   */
  def setId(id: GradoopId): Unit
}
