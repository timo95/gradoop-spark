package org.gradoop.common.model.api.gve

import org.gradoop.common.model.api.elements.ElementFactory

trait GveGraphHeadFactory[G <: GveGraphHead] extends ElementFactory[G] {

  /** Creates a new graph head based.
   *
   * @return graph data
   */
  def create: G

  /** Initializes a graph head based on the given parameters.
   *
   * @param id graph identifier
   * @return graph data
   */
  def apply(id: Id): G

  /** Creates a new graph head based on the given parameters.
   *
   * @param label graph label
   * @return graph data
   */
  def create(label: Label): G

  /** Initializes a graph head based on the given parameters.
   *
   * @param id    graph identifier
   * @param label graph label
   * @return graph data
   */
  def apply(id: Id, label: Label): G

  /** Creates a new graph head based on the given parameters.
   *
   * @param label      graph label
   * @param properties graph attributes
   * @return graph data
   */
  def create(label: Label, properties: Properties): G

  /** Initializes a graph head based on the given parameters.
   *
   * @param id         graph identifier
   * @param label      graph label
   * @param properties graph attributes
   * @return graph data
   */
  def apply(id: Id, label: Label, properties: Properties): G
}
