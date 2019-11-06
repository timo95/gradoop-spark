package org.gradoop.spark.model.impl.elements

import org.apache.spark.sql.{Encoder, Encoders}
import org.gradoop.common.model.api.elements.{Edge, EdgeFactory}
import org.gradoop.common.model.impl.id.GradoopId

class EpgmEdge(id: Id, labels: Labels, var sourceId: Id, var targetId: Id, properties: Properties, graphIds: IdSet)
  extends EpgmGraphElement(id, labels, properties, graphIds) with Edge {

  override def getSourceId: Id = sourceId

  override def getTargetId: Id = targetId

  override def setSourceId(sourceId: Id): Unit = {
    this.sourceId = sourceId
  }

  override def setTargetId(targetId: Id): Unit = {
    this.targetId = targetId
  }
}

object EpgmEdge extends EdgeFactory[E] {

  def getEncoder: Encoder[EpgmEdge] = Encoders.kryo(classOf[EpgmEdge])

  override def getType: Class[E] = classOf[EpgmEdge]

  override def apply(id: Id): E = apply(id, new Labels(""), GradoopId.NULL_VALUE, GradoopId.NULL_VALUE)

  override def create(sourceId: Id, targetId: Id): E = apply(GradoopId.get, sourceId, targetId)

  override def apply(id: Id, sourceId: Id, targetId: Id): E = apply(id, new Labels(""), sourceId, targetId)

  override def create(labels: Labels, sourceId: Id, targetId: Id): E = apply(GradoopId.get, labels, sourceId, targetId)

  override def apply(id: Id, labels: Labels, sourceId: Id, targetId: Id): E = apply(id, labels, sourceId, targetId, null, null)

  override def create(labels: Labels, sourceId: Id, targetId: Id, properties: Properties): E = apply(GradoopId.get, labels, sourceId, targetId, properties)

  override def apply(id: Id, labels: Labels, sourceId: Id, targetId: Id, properties: Properties): E = apply(id, labels, sourceId, targetId, properties, null)

  override def create(labels: Labels, sourceId: Id, targetId: Id, graphIds: IdSet): E = apply(GradoopId.get, labels, sourceId, targetId, graphIds)

  override def apply(id: Id, labels: Labels, sourceId: Id, targetId: Id, graphIds: IdSet): E = apply(id, labels, sourceId, targetId, null, graphIds)

  override def create(labels: Labels, sourceId: Id, targetId: Id, properties: Properties, graphIds: IdSet): E = apply(GradoopId.get, labels, sourceId, targetId, properties, graphIds)

  override def apply(id: Id, labels: Labels, sourceId: Id, targetId: Id, properties: Properties, graphIds: IdSet): EpgmEdge = new EpgmEdge(id, labels, sourceId, targetId, properties, graphIds)
}