package org.gradoop.spark.model.impl.gve

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.gradoop.common.id.GradoopId
import org.gradoop.common.model.api.gve.{GveGraphHead, GveGraphHeadFactory}

final case class EpgmGveGraphHead(var id: Id, var label: Label, var properties: Properties) extends GveGraphHead

object EpgmGveGraphHead extends GveGraphHeadFactory[L#G] {

  def encoder: Encoder[L#G] = ExpressionEncoder[L#G]

  override def producedType: Class[L#G] = classOf[L#G]

  override def create: L#G = apply(GradoopId.get)

  override def apply(id: Id): L#G = apply(id, "")

  override def create(label: Label): L#G = apply(GradoopId.get, label)

  override def apply(id: Id, label: Label): L#G = apply(id, label, Map.empty)

  override def create(label: Label, properties: Properties): L#G = apply(GradoopId.get, label, properties)

  override def apply(id: Id, label: Label, properties: Properties): L#G = new EpgmGveGraphHead(id, label, properties)
}
