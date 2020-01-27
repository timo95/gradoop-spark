package org.gradoop.common.model.api.gve

import org.gradoop.common.model.api.elements.ElementFactory

trait GveVertexFactory[V <: GveVertex] extends ElementFactory[V] {

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
   * @param label      vertex label
   * @param properties vertex properties
   * @return vertex data
   */
  def create(label: Label, properties: Properties): V

  /** Initializes a vertex based on the given parameters.
   *
   * @param id         vertex identifier
   * @param label      vertex label
   * @param properties vertex properties
   * @return vertex data
   */
  def apply(id: Id, label: Label, properties: Properties): V

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

  /** Creates a new vertex based on the given parameters.
   *
   * @param label      vertex label
   * @param properties vertex properties
   * @param graphIds   graphIds, that contain the vertex
   * @return vertex data
   */
  def create(label: Label, properties: Properties, graphIds: IdSet): V

  /** Initializes a vertex based on the given parameters.
   *
   * @param id         vertex identifier
   * @param label      vertex label
   * @param properties vertex properties
   * @param graphIds   graphIds, that contain the vertex
   * @return vertex data
   */
  def apply(id: Id, label: Label, properties: Properties, graphIds: IdSet): V
}
