package org.gradoop.common.model.api.elements

trait VertexFactory[V <: Vertex] extends ElementFactory[V] {

  /**
   * Initializes a new vertex based on the given parameters.
   *
   * @return vertex data
   */
  def create: V

  /**
   * Initializes a vertex based on the given parameters.
   *
   * @param id vertex identifier
   * @return vertex data
   */
  def apply(id: Id): V

  /**
   * Creates a new vertex based on the given parameters.
   *
   * @param labels vertex labels
   * @return vertex data
   */
  def create(labels: Labels): V

  /**
   * Initializes a vertex based on the given parameters.
   *
   * @param id    vertex identifier
   * @param labels vertex labels
   * @return vertex data
   */
  def apply(id: Id, labels: Labels): V

  /**
   * Creates a new vertex based on the given parameters.
   *
   * @param labels      vertex labels
   * @param properties vertex properties
   * @return vertex data
   */
  def create(labels: Labels, properties: Properties): V

  /**
   * Initializes a vertex based on the given parameters.
   *
   * @param id         vertex identifier
   * @param labels      vertex labels
   * @param properties vertex properties
   * @return vertex data
   */
  def apply(id: Id, labels: Labels, properties: Properties): V

  /**
   * Creates a new vertex based on the given parameters.
   *
   * @param labels    vertex labels
   * @param graphIds graphIds, that contain the vertex
   * @return vertex data
   */
  def create(labels: Labels, graphIds: IdSet): V

  /**
   * Initializes a vertex based on the given parameters.
   *
   * @param id       vertex identifier
   * @param labels    vertex labels
   * @param graphIds graphIds, that contain the vertex
   * @return vertex data
   */
  def apply(id: Id, labels: Labels, graphIds: IdSet): V

  /**
   * Creates a new vertex based on the given parameters.
   *
   * @param labels      vertex labels
   * @param properties vertex properties
   * @param graphIds   graphIds, that contain the vertex
   * @return vertex data
   */
  def create(labels: Labels, properties: Properties, graphIds: IdSet): V

  /**
   * Initializes a vertex based on the given parameters.
   *
   * @param id         vertex identifier
   * @param labels      vertex labels
   * @param properties vertex properties
   * @param graphIds   graphIds, that contain the vertex
   * @return vertex data
   */
  def apply(id: Id, labels: Labels, properties: Properties, graphIds: IdSet): V
}
