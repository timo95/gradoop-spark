package org.gradoop.spark.model.impl.gve

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.gradoop.common.id.GradoopId
import org.gradoop.common.model.api.gve.{GveEdge, GveEdgeFactory}

final case class EpgmGveEdge(var id: Id, var label: Label, var sourceId: Id, var targetId: Id,
  var properties: Properties, var graphIds: IdSet) extends GveEdge

object EpgmGveEdge extends GveEdgeFactory[L#E] {

  def encoder: Encoder[L#E] = ExpressionEncoder[L#E]

  override def producedType: Class[L#E] = classOf[L#E]

  override def create(sourceId: Id, targetId: Id): L#E = apply(GradoopId.get, sourceId, targetId)

  override def apply(id: Id, sourceId: Id, targetId: Id): L#E = apply(id, "", sourceId, targetId)

  override def create(label: Label, sourceId: Id, targetId: Id): L#E = apply(GradoopId.get, label, sourceId, targetId)

  override def apply(id: Id, label: Label, sourceId: Id, targetId: Id): L#E = {
    apply(id, label, sourceId, targetId, Map.empty, Set.empty)
  }

  override def create(label: Label, sourceId: Id, targetId: Id, properties: Properties): L#E = {
    apply(GradoopId.get, label, sourceId, targetId, properties)
  }

  override def apply(id: Id, label: Label, sourceId: Id, targetId: Id, properties: Properties): L#E = {
    apply(id, label, sourceId, targetId, properties, Set.empty)
  }

  override def create(label: Label, sourceId: Id, targetId: Id, graphIds: IdSet): L#E = {
    apply(GradoopId.get, label, sourceId, targetId, graphIds)
  }

  override def apply(id: Id, label: Label, sourceId: Id, targetId: Id, graphIds: IdSet): L#E = {
    apply(id, label, sourceId, targetId, Map.empty, graphIds)
  }

  override def create(label: Label, sourceId: Id, targetId: Id, properties: Properties, graphIds: IdSet): L#E = {
    apply(GradoopId.get, label, sourceId, targetId, properties, graphIds)
  }

  override def apply(id: Id, label: Label, sourceId: Id, targetId: Id, properties: Properties, graphIds: IdSet):
  EpgmGveEdge = {
    new EpgmGveEdge(id, label, sourceId, targetId, properties, graphIds)
  }
}
