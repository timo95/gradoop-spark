package org.gradoop.spark.model.api.elements

trait ElementFactory[EL >: Element] {
  /**
   * Returns the type of the instances produced by that factory.
   *
   * @return produced entity type
   */
  def getType: Class[EL] = classOf[EL]
}
