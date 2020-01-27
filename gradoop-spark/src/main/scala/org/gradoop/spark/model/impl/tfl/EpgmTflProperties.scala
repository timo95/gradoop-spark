package org.gradoop.spark.model.impl.tfl

import org.apache.spark.sql.Encoder
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.gradoop.common.model.api.tfl.{TflProperties, TflPropertiesFactory}

case class EpgmTflProperties(var id: Id, var label: Label, var properties: Properties) extends TflProperties

object EpgmTflProperties extends TflPropertiesFactory[L#P] {

  def encoder: Encoder[L#P] = ExpressionEncoder[L#P]

  override def producedType: Class[L#P] = classOf[L#P]

  override def apply(id: Id, label: Label, properties: Properties): L#P = {
    new EpgmTflProperties(id, label, properties)
  }
}
