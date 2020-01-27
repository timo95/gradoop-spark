package org.gradoop.common.model.api.tfl

import org.gradoop.common.model.api.elements.ElementFactory

trait TflVertexFactory[V <: TflVertex] extends ElementFactory[V] {

  /** Initializes a new vertex based on the given parameters.
   *
   * @return vertex data
   */
  def create: V

  /** Initializes a vertex based on the given parameters.
   *
   * @param id vertex identifier
   * @return vertex data
   */
  def apply(id: Id): V

  /** Creates a new vertex based on the given parameters.
   *
   * @param label vertex label
   * @return vertex data
   */
  def create(label: Label): V

  /** Initializes a vertex based on the given parameters.
   *
   * @param id    vertex identifier
   * @param label vertex label
   * @return vertex data
   */
  def apply(id: Id, label: Label): V

  /** Creates a new vertex based on the given parameters.
   *
   * @param label    vertex label
   * @param graphIds graphIds, that contain the vertex
   * @return vertex data
   */
  def create(label: Label, graphIds: IdSet): V

  /** Initializes a vertex based on the given parameters.
   *
   * @param id       vertex identifier
   * @param label    vertex label
   * @param graphIds graphIds, that contain the vertex
   * @return vertex data
   */
  def apply(id: Id, label: Label, graphIds: IdSet): V
}
