package org.gradoop.spark.model.impl.elements

import org.gradoop.spark.model.api.elements.{Edge, EdgeFactory}
import org.gradoop.spark.util.GradoopId

case class EpgmEdge(id: Id, labels: Labels, sourceId: Id, targetId: Id, properties: Properties, graphIds: IdSet) extends EpgmGraphElement(id, labels, properties, graphIds) with Edge {
  override def getSourceId: Id = sourceId
  override def getTargetId: Id = targetId
}

object EpgmEdge extends EdgeFactory[E] {

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param sourceId source vertex id
   * @param targetId target vertex id
   * @return edge data
   */
  override def create(sourceId: Id, targetId: Id): E = apply(GradoopId.get, sourceId, targetId)

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param sourceId source vertex id
   * @param targetId target vertex id
   * @return edge data
   */
  override def apply(id: Id, sourceId: Id, targetId: Id): E = apply(id, new Labels(0), sourceId, targetId)

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param sourceId source vertex id
   * @param targetId target vertex id
   * @param labels          edge labels
   * @return edge data
   */
  override def create(labels: Labels, sourceId: Id, targetId: Id): E = apply(GradoopId.get, labels, sourceId, targetId)

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param sourceId source vertex id
   * @param targetId target vertex id
   * @param labels          edge labels
   * @return edge data
   */
  override def apply(id: Id, labels: Labels, sourceId: Id, targetId: Id): E = apply(id, labels, sourceId, targetId, null, null)

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param labels          edge labels
   * @param sourceId source vertex id
   * @param targetId target vertex id
   * @param properties     edge properties
   * @return edge data
   */
  override def create(labels: Labels, sourceId: Id, targetId: Id, properties: Properties): E = apply(GradoopId.get, labels, sourceId, targetId, properties)

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param labels          edge labels
   * @param sourceId source vertex id
   * @param targetId target vertex id
   * @param properties     edge properties
   * @return edge data
   */
  override def apply(id: Id, labels: Labels, sourceId: Id, targetId: Id, properties: Properties): E = apply(id, labels, sourceId, targetId, properties, null)

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param labels          edge labels
   * @param sourceId source vertex id
   * @param targetId target vertex id
   * @param graphIds       graphIds, that contain the edge
   * @return edge data
   */
  override def create(labels: Labels, sourceId: Id, targetId: Id, graphIds: IdSet): E = apply(GradoopId.get, labels, sourceId, targetId, graphIds)

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param labels          edge labels
   * @param sourceId source vertex id
   * @param targetId target vertex id
   * @param graphIds       graphIds, that contain the edge
   * @return edge data
   */
  override def apply(id: Id, labels: Labels, sourceId: Id, targetId: Id, graphIds: IdSet): E = apply(id, labels, sourceId, targetId, null, graphIds)

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param labels          edge labels
   * @param sourceId source vertex id
   * @param targetId target vertex id
   * @param properties     edge properties
   * @param graphIds       graphIds, that contain the edge
   * @return edge data
   */
  override def create(labels: Labels, sourceId: Id, targetId: Id, properties: Properties, graphIds: IdSet): E = apply(GradoopId.get, labels, sourceId, targetId, properties, graphIds)

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param labels          edge labels
   * @param sourceId source vertex id
   * @param targetId target vertex id
   * @param properties     edge properties
   * @param graphIds       graphIds, that contain the edge
   * @return edge data
   */
  override def apply(id: Id, labels: Labels, sourceId: Id, targetId: Id, properties: Properties, graphIds: IdSet): EpgmEdge = new EpgmEdge(id, labels, sourceId, targetId, properties, graphIds)
}