package org.gradoop.common.model.api.gve

import org.gradoop.common.model.api.elements.ElementFactory

trait GveEdgeFactory[E <: GveEdge] extends ElementFactory[E] {

  /** Creates a new edge based on the given parameters.
   *
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @return edge data
   */
  def create(sourceVertexId: Id, targetVertexId: Id): E

  /** Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @return edge data
   */
  def apply(id: Id, sourceVertexId: Id, targetVertexId: Id): E

  /** Creates a new edge based on the given parameters.
   *
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param label          edge label
   * @return edge data
   */
  def create(label: Label, sourceVertexId: Id, targetVertexId: Id): E

  /** Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param label          edge label
   * @return edge data
   */
  def apply(id: Id, label: Label, sourceVertexId: Id, targetVertexId: Id): E

  /** Creates a new edge based on the given parameters.
   *
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param properties     edge properties
   * @return edge data
   */
  def create(label: Label, sourceVertexId: Id, targetVertexId: Id, properties: Properties): E

  /** Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param properties     edge properties
   * @return edge data
   */
  def apply(id: Id, label: Label, sourceVertexId: Id, targetVertexId: Id, properties: Properties): E

  /** Creates a new edge based on the given parameters.
   *
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param graphIds       graphIds, that contain the edge
   * @return edge data
   */
  def create(label: Label, sourceVertexId: Id, targetVertexId: Id, graphIds: IdSet): E

  /** Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param graphIds       graphIds, that contain the edge
   * @return edge data
   */
  def apply(id: Id, label: Label, sourceVertexId: Id, targetVertexId: Id, graphIds: IdSet): E

  /** Creates a new edge based on the given parameters.
   *
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param properties     edge properties
   * @param graphIds       graphIds, that contain the edge
   * @return edge data
   */
  def create(label: Label, sourceVertexId: Id, targetVertexId: Id, properties: Properties, graphIds: IdSet): E

  /** Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param label          edge label
   * @param sourceVertexId source vertex id
   * @param targetVertexId target vertex id
   * @param properties     edge properties
   * @param graphIds       graphIds, that contain the edge
   * @return edge data
   */
  def apply(id: Id, label: Label, sourceVertexId: Id, targetVertexId: Id, properties: Properties, graphIds: IdSet): E
}
