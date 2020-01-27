package org.gradoop.spark.model.impl.tfl

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.gradoop.common.id.GradoopId
import org.gradoop.common.model.api.tfl.{TflGraphHead, TflGraphHeadFactory}

final case class EpgmTflGraphHead(var id: Id, var label: Label) extends TflGraphHead

object EpgmTflGraphHead extends TflGraphHeadFactory[L#G] {

  def encoder: Encoder[L#G] = ExpressionEncoder[L#G]

  override def producedType: Class[L#G] = classOf[L#G]

  override def create: L#G = apply(GradoopId.get)

  override def apply(id: Id): L#G = apply(id, "")

  override def create(label: Label): L#G = apply(GradoopId.get, label)

  override def apply(id: Id, label: Label): L#G = new EpgmTflGraphHead(id, label)
}
