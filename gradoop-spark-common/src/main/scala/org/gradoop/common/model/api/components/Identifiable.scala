package org.gradoop.common.model.api.components

import org.gradoop.common.model.impl.id.GradoopId

trait Identifiable extends Serializable {

  /** Returns the identifier of that entity.
   *
   * @return identifier
   */
  def id: Id

  /** Sets the identifier of that entity.
   *
   * @param id identifier
   */
  def id_=(id: GradoopId): Unit
}
