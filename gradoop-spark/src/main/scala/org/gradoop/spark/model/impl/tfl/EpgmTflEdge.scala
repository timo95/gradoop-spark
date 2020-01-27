package org.gradoop.spark.model.impl.tfl

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.gradoop.common.id.GradoopId
import org.gradoop.common.model.api.tfl.{TflEdge, TflEdgeFactory}

final case class EpgmTflEdge(var id: Id, var label: Label, var sourceId: Id, var targetId: Id, var graphIds: IdSet)
  extends TflEdge

object EpgmTflEdge  extends TflEdgeFactory[L#E] {

  def encoder: Encoder[L#E] = ExpressionEncoder[L#E]

  override def producedType: Class[L#E] = classOf[L#E]

  override def create(sourceId: Id, targetId: Id): L#E = apply(GradoopId.get, sourceId, targetId)

  override def apply(id: Id, sourceId: Id, targetId: Id): L#E = apply(id, "", sourceId, targetId)

  override def create(label: Label, sourceId: Id, targetId: Id): L#E = apply(GradoopId.get, label, sourceId, targetId)

  override def apply(id: Id, label: Label, sourceId: Id, targetId: Id): L#E = {
    apply(id, label, sourceId, targetId, Set.empty)
  }

  override def create(label: Label, sourceId: Id, targetId: Id, graphIds: IdSet): L#E = {
    apply(GradoopId.get, label, sourceId, targetId, graphIds)
  }

  override def apply(id: Id, label: Label, sourceId: Id, targetId: Id, graphIds: IdSet): L#E = {
    new EpgmTflEdge(id, label, sourceId, targetId, graphIds)
  }
}
