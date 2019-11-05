package org.gradoop.spark.model.impl.elements

import org.apache.spark.sql.{Encoder, Encoders}
import org.gradoop.common.model.api.elements.{Vertex, VertexFactory}
import org.gradoop.common.model.impl.id.GradoopId

class EpgmVertex(id: Id, labels: Labels, properties: Properties, graphIds: IdSet)
  extends EpgmGraphElement(id, labels, properties, graphIds) with Vertex

object EpgmVertex extends VertexFactory[V] {

  def getEncoder: Encoder[EpgmVertex] = Encoders.kryo(classOf[EpgmVertex])

  override def getType: Class[V] = classOf[EpgmVertex]

  /** Creates a new edge based on the given parameters.
   *
   * @return edge data
   */
  override def create: V = apply(GradoopId.get)

  /** Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @return edge data
   */
  override def apply(id: Id): V = apply(id, new Labels(0))

  /** Creates a new edge based on the given parameters.
   *
   * @param labels          edge labels
   * @return edge data
   */
  override def create(labels: Labels): V = apply(GradoopId.get, labels)

  /** Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param labels          edge labels
   * @return edge data
   */
  override def apply(id: Id, labels: Labels): V = apply(id, labels, null, null)

  /** Creates a new edge based on the given parameters.
   *
   * @param labels          edge labels
   * @param properties     edge properties
   * @return edge data
   */
  override def create(labels: Labels, properties: Properties): V = apply(GradoopId.get, labels, properties)

  /** Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param labels          edge labels
   * @param properties     edge properties
   * @return edge data
   */
  override def apply(id: Id, labels: Labels, properties: Properties): V = apply(id, labels, properties, null)

  /** Creates a new edge based on the given parameters.
   *
   * @param labels          edge labels
   * @param graphIds       graphIds, that contain the edge
   * @return edge data
   */
  override def create(labels: Labels, graphIds: IdSet): V = apply(GradoopId.get, labels, graphIds)

  /** Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param labels         edge labels
   * @param graphIds       graphIds, that contain the edge
   * @return edge data
   */
  override def apply(id: Id, labels: Labels, graphIds: IdSet): V = apply(id, labels, null, graphIds)

  /** Creates a new edge based on the given parameters.
   *
   * @param labels         edge labels
   * @param properties     edge properties
   * @param graphIds       graphIds, that contain the edge
   * @return edge data
   */
  override def create(labels: Labels, properties: Properties, graphIds: IdSet): V = apply(GradoopId.get, labels, properties, graphIds)

  /** Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param labels         edge labels
   * @param properties     edge properties
   * @param graphIds       graphIds, that contain the edge
   * @return edge data
   */
  override def apply(id: Id, labels: Labels, properties: Properties, graphIds: IdSet): V = new EpgmVertex(id, labels, properties, graphIds)
}