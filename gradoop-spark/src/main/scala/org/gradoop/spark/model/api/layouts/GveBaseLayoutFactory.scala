package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Encoder
import org.gradoop.common.model.api.gve.ElementFactoryProvider
import org.gradoop.spark.model.impl.types.GveLayoutType

trait GveBaseLayoutFactory[L <: GveLayoutType] extends LogicalGraphLayoutFactory[L] with GraphCollectionLayoutFactory[L]
  with ElementFactoryProvider[L#G, L#V, L#E] {

  def graphHeadEncoder: Encoder[L#G]

  def vertexEncoder: Encoder[L#V]

  def edgeEncoder: Encoder[L#E]
}
