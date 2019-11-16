package org.gradoop.spark.model.impl.elements

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.gradoop.common.model.api.elements.{Edge, EdgeFactory}
import org.gradoop.common.model.impl.id.GradoopId

final case class EpgmEdge(var id: Id, var label: Label, var sourceId: Id, var targetId: Id,
                          var properties: Properties, var graphIds: IdSet) extends Edge

object EpgmEdge extends EdgeFactory[L#E] {

  def encoder: Encoder[L#E] = ExpressionEncoder[L#E]

  override def producedType: Class[L#E] = classOf[L#E]

  override def apply(id: Id): L#E = apply(id, new Label(""), GradoopId.NULL_VALUE, GradoopId.NULL_VALUE)

  override def create(sourceId: Id, targetId: Id): L#E = apply(GradoopId.get, sourceId, targetId)

  override def apply(id: Id, sourceId: Id, targetId: Id): L#E = apply(id, new Label(""), sourceId, targetId)

  override def create(labels: Label, sourceId: Id, targetId: Id): L#E = apply(GradoopId.get, labels, sourceId, targetId)

  override def apply(id: Id, labels: Label, sourceId: Id, targetId: Id): L#E = apply(id, labels, sourceId, targetId, null, null)

  override def create(labels: Label, sourceId: Id, targetId: Id, properties: Properties): L#E = apply(GradoopId.get, labels, sourceId, targetId, properties)

  override def apply(id: Id, labels: Label, sourceId: Id, targetId: Id, properties: Properties): L#E = apply(id, labels, sourceId, targetId, properties, null)

  override def create(labels: Label, sourceId: Id, targetId: Id, graphIds: IdSet): L#E = apply(GradoopId.get, labels, sourceId, targetId, graphIds)

  override def apply(id: Id, labels: Label, sourceId: Id, targetId: Id, graphIds: IdSet): L#E = apply(id, labels, sourceId, targetId, null, graphIds)

  override def create(labels: Label, sourceId: Id, targetId: Id, properties: Properties, graphIds: IdSet): L#E = apply(GradoopId.get, labels, sourceId, targetId, properties, graphIds)

  override def apply(id: Id, labels: Label, sourceId: Id, targetId: Id, properties: Properties, graphIds: IdSet): EpgmEdge = new EpgmEdge(id, labels, sourceId, targetId, properties, graphIds)
}