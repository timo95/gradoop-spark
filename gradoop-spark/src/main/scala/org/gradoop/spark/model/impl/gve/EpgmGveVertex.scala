package org.gradoop.spark.model.impl.gve

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.gradoop.common.id.GradoopId
import org.gradoop.common.model.api.gve.{GveVertex, GveVertexFactory}

final case class EpgmGveVertex(var id: Id, var label: Label, var properties: Properties, var graphIds: IdSet)
  extends GveVertex

object EpgmGveVertex extends GveVertexFactory[L#V] {

  def encoder: Encoder[L#V] = ExpressionEncoder[L#V]

  override def producedType: Class[L#V] = classOf[L#V]

  override def create: L#V = apply(GradoopId.get)

  override def apply(id: Id): L#V = apply(id, "")

  override def create(label: Label): L#V = apply(GradoopId.get, label)

  override def apply(id: Id, label: Label): L#V = apply(id, label, Map.empty, Set.empty)

  override def create(label: Label, properties: Properties): L#V = apply(GradoopId.get, label, properties)

  override def apply(id: Id, label: Label, properties: Properties): L#V = apply(id, label, properties, Set.empty)

  override def create(label: Label, graphIds: IdSet): L#V = apply(GradoopId.get, label, graphIds)

  override def apply(id: Id, label: Label, graphIds: IdSet): L#V = apply(id, label, Map.empty, graphIds)

  override def create(label: Label, properties: Properties, graphIds: IdSet): L#V = {
    apply(GradoopId.get, label, properties, graphIds)
  }

  override def apply(id: Id, label: Label, properties: Properties, graphIds: IdSet): L#V = {
    new EpgmGveVertex(id, label, properties, graphIds)
  }
}
