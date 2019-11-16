package org.gradoop.spark.model.impl.layouts

import org.apache.spark.sql.Encoder
import org.gradoop.common.model.api.elements.{EdgeFactory, GraphHeadFactory, VertexFactory}
import org.gradoop.spark.model.api.layouts.BaseLayoutFactory
import org.gradoop.spark.model.impl.elements.{EpgmEdge, EpgmGraphHead, EpgmVertex}

trait EpgmBaseLayoutFactory extends BaseLayoutFactory[L] {

  override def graphHeadEncoder: Encoder[L#G] = EpgmGraphHead.encoder

  override def vertexEncoder: Encoder[L#V] = EpgmVertex.encoder

  override def edgeEncoder: Encoder[L#E] = EpgmEdge.encoder

  override def graphHeadFactory: GraphHeadFactory[L#G] = EpgmGraphHead

  override def vertexFactory: VertexFactory[L#V] = EpgmVertex

  override def edgeFactory: EdgeFactory[L#E] = EpgmEdge
}
