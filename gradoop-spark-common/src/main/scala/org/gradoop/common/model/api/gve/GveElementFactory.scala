package org.gradoop.common.model.api.gve

trait GveElementFactory[EL <: GveElement] {

  /** Returns the type of the instances produced by that factory.
   *
   * @return produced entity type
   */
  def producedType: Class[EL]
}
