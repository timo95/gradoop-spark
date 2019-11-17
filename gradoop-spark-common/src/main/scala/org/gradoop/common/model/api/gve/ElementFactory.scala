package org.gradoop.common.model.api.gve

trait ElementFactory[EL <: Element] {

  /** Returns the type of the instances produced by that factory.
   *
   * @return produced entity type
   */
  def producedType: Class[EL]
}
