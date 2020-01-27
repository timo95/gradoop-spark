package org.gradoop.spark.model.impl.tfl

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.gradoop.common.id.GradoopId
import org.gradoop.common.model.api.tfl.{TflVertex, TflVertexFactory}

final case class EpgmTflVertex(var id: Id, var label: Label, var graphIds: IdSet) extends TflVertex

object EpgmTflVertex extends TflVertexFactory[L#V] {

  def encoder: Encoder[L#V] = ExpressionEncoder[L#V]

  override def producedType: Class[L#V] = classOf[L#V]

  override def create: L#V = apply(GradoopId.get)

  override def apply(id: Id): L#V = apply(id, "")

  override def create(label: Label): L#V = apply(GradoopId.get, label)

  override def apply(id: Id, label: Label): L#V = apply(id, label, Set.empty)

  override def create(label: Label, graphIds: IdSet): L#V = apply(GradoopId.get, label, graphIds)

  override def apply(id: Id, label: Label, graphIds: IdSet): L#V = new EpgmTflVertex(id, label, graphIds)
}
