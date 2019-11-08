package org.gradoop.spark.model.impl.elements

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.gradoop.common.model.api.elements.{GraphHead, GraphHeadFactory}
import org.gradoop.common.model.impl.id.GradoopId

final case class EpgmGraphHead(var id: Id, var labels: Labels, var properties: Properties) extends GraphHead

object EpgmGraphHead extends GraphHeadFactory[G] {

  def encoder: Encoder[G] = ExpressionEncoder[G]

  override def producedType: Class[G] = classOf[G]

  override def create: G = apply(GradoopId.get)

  override def apply(id: Id): G = apply(id, new Labels(""))

  override def create(labels: Labels): G = apply(GradoopId.get, labels)

  override def apply(id: Id, labels: Labels): G = apply(id, labels, Map[String, PV]())

  override def create(labels: Labels, properties: Properties): G = apply(GradoopId.get, labels, properties)

  override def apply(id: Id, labels: Labels, properties: Properties): G = new EpgmGraphHead(id, labels, properties)
}