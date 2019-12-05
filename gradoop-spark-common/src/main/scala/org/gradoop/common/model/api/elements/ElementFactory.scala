package org.gradoop.common.model.api.elements

trait ElementFactory[EL] {

  /** Returns the type of the instances produced by that factory.
   *
   * @return produced entity type
   */
  def producedType: Class[EL]
}
