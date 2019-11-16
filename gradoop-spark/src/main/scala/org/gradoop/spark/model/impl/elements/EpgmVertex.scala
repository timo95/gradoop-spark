package org.gradoop.spark.model.impl.elements

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.gradoop.common.model.api.elements.{Vertex, VertexFactory}
import org.gradoop.common.model.impl.id.GradoopId

final case class EpgmVertex(var id: Id, var label: Label, var properties: Properties, var graphIds: IdSet)
  extends Vertex

object EpgmVertex extends VertexFactory[L#V] {

  def encoder: Encoder[L#V] = ExpressionEncoder[L#V]

  override def producedType: Class[L#V] = classOf[L#V]

  override def create: L#V = apply(GradoopId.get)

  override def apply(id: Id): L#V = apply(id, new Label(""))

  override def create(labels: Label): L#V = apply(GradoopId.get, labels)

  override def apply(id: Id, labels: Label): L#V = apply(id, labels, null, null)

  override def create(labels: Label, properties: Properties): L#V = apply(GradoopId.get, labels, properties)

  override def apply(id: Id, labels: Label, properties: Properties): L#V = apply(id, labels, properties, null)

  override def create(labels: Label, graphIds: IdSet): L#V = apply(GradoopId.get, labels, graphIds)

  override def apply(id: Id, labels: Label, graphIds: IdSet): L#V = apply(id, labels, null, graphIds)

  override def create(labels: Label, properties: Properties, graphIds: IdSet): L#V = apply(GradoopId.get, labels, properties, graphIds)

  override def apply(id: Id, labels: Label, properties: Properties, graphIds: IdSet): L#V = new EpgmVertex(id, labels, properties, graphIds)
}