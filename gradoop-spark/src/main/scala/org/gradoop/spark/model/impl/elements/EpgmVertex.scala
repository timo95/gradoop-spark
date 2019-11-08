package org.gradoop.spark.model.impl.elements

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.gradoop.common.model.api.elements.{Vertex, VertexFactory}
import org.gradoop.common.model.impl.id.GradoopId

final case class EpgmVertex(var id: Id, var labels: Labels, var properties: Properties, var graphIds: IdSet)
  extends Vertex

object EpgmVertex extends VertexFactory[V] {

  def encoder: Encoder[V] = ExpressionEncoder[V]

  override def producedType: Class[V] = classOf[V]

  override def create: V = apply(GradoopId.get)

  override def apply(id: Id): V = apply(id, new Labels(""))

  override def create(labels: Labels): V = apply(GradoopId.get, labels)

  override def apply(id: Id, labels: Labels): V = apply(id, labels, null, null)

  override def create(labels: Labels, properties: Properties): V = apply(GradoopId.get, labels, properties)

  override def apply(id: Id, labels: Labels, properties: Properties): V = apply(id, labels, properties, null)

  override def create(labels: Labels, graphIds: IdSet): V = apply(GradoopId.get, labels, graphIds)

  override def apply(id: Id, labels: Labels, graphIds: IdSet): V = apply(id, labels, null, graphIds)

  override def create(labels: Labels, properties: Properties, graphIds: IdSet): V = apply(GradoopId.get, labels, properties, graphIds)

  override def apply(id: Id, labels: Labels, properties: Properties, graphIds: IdSet): V = new EpgmVertex(id, labels, properties, graphIds)
}