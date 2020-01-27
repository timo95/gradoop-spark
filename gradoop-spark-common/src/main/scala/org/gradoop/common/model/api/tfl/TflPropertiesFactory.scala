package org.gradoop.common.model.api.tfl

import org.gradoop.common.model.api.elements.ElementFactory

trait TflPropertiesFactory[P <: TflProperties] extends ElementFactory[P] {

  /** Initializes a properties element based on the given parameters.
   *
   * @param id         graph identifier
   * @param label      graph label
   * @param properties graph attributes
   * @return graph data
   */
  def apply(id: Id, label: Label, properties: Properties): P
}
