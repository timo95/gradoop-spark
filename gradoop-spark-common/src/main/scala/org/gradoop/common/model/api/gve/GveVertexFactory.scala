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
   * @param labels vertex labels
   * @return vertex data
   */
  def create(labels: Label): V

  /** Initializes a vertex based on the given parameters.
   *
   * @param id    vertex identifier
   * @param labels vertex labels
   * @return vertex data
   */
  def apply(id: Id, labels: Label): V

  /** Creates a new vertex based on the given parameters.
   *
   * @param labels      vertex labels
   * @param properties vertex properties
   * @return vertex data
   */
  def create(labels: Label, properties: Properties): V

  /** Initializes a vertex based on the given parameters.
   *
   * @param id         vertex identifier
   * @param labels      vertex labels
   * @param properties vertex properties
   * @return vertex data
   */
  def apply(id: Id, labels: Label, properties: Properties): V

  /** Creates a new vertex based on the given parameters.
   *
   * @param labels    vertex labels
   * @param graphIds graphIds, that contain the vertex
   * @return vertex data
   */
  def create(labels: Label, graphIds: IdSet): V

  /** Initializes a vertex based on the given parameters.
   *
   * @param id       vertex identifier
   * @param labels    vertex labels
   * @param graphIds graphIds, that contain the vertex
   * @return vertex data
   */
  def apply(id: Id, labels: Label, graphIds: IdSet): V

  /** Creates a new vertex based on the given parameters.
   *
   * @param labels      vertex labels
   * @param properties vertex properties
   * @param graphIds   graphIds, that contain the vertex
   * @return vertex data
   */
  def create(labels: Label, properties: Properties, graphIds: IdSet): V

  /** Initializes a vertex based on the given parameters.
   *
   * @param id         vertex identifier
   * @param labels      vertex labels
   * @param properties vertex properties
   * @param graphIds   graphIds, that contain the vertex
   * @return vertex data
   */
  def apply(id: Id, labels: Label, properties: Properties, graphIds: IdSet): V
}
