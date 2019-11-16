package org.gradoop.spark.model.impl.elements

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.gradoop.common.model.api.elements.{GraphHead, GraphHeadFactory}
import org.gradoop.common.model.impl.id.GradoopId

final case class EpgmGraphHead(var id: Id, var label: Label, var properties: Properties) extends GraphHead

object EpgmGraphHead extends GraphHeadFactory[L#G] {

  def encoder: Encoder[L#G] = ExpressionEncoder[L#G]

  override def producedType: Class[L#G] = classOf[L#G]

  override def create: L#G = apply(GradoopId.get)

  override def apply(id: Id): L#G = apply(id, new Label(""))

  override def create(labels: Label): L#G = apply(GradoopId.get, labels)

  override def apply(id: Id, labels: Label): L#G = apply(id, labels, Map[String, PV]())

  override def create(labels: Label, properties: Properties): L#G = apply(GradoopId.get, labels, properties)

  override def apply(id: Id, labels: Label, properties: Properties): L#G = new EpgmGraphHead(id, labels, properties)
}