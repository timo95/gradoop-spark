package org.gradoop.common.model.api.gve

import org.gradoop.common.model.api.components.{Id, Label, Properties}

trait GraphHeadFactory[G <: GraphHead] extends ElementFactory[G] {

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
   * @param labels graph labels
   * @return graph data
   */
  def create(labels: Label): G

  /** Initializes a graph head based on the given parameters.
   *
   * @param id    graph identifier
   * @param labels graph labels
   * @return graph data
   */
  def apply(id: Id, labels: Label): G

  /** Creates a new graph head based on the given parameters.
   *
   * @param labels      graph labels
   * @param properties graph attributes
   * @return graph data
   */
  def create(labels: Label, properties: Properties): G

  /** Initializes a graph head based on the given parameters.
   *
   * @param id         graph identifier
   * @param labels      graph labels
   * @param properties graph attributes
   * @return graph data
   */
  def apply(id: Id, labels: Label, properties: Properties): G
}
