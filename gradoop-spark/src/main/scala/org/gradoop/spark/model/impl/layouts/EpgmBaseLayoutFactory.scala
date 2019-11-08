package org.gradoop.spark.model.impl.layouts

import org.apache.spark.sql.Encoder
import org.gradoop.common.model.api.elements.{EdgeFactory, GraphHeadFactory, VertexFactory}
import org.gradoop.spark.model.api.layouts.BaseLayoutFactory
import org.gradoop.spark.model.impl.elements.{EpgmEdge, EpgmGraphHead, EpgmVertex}

trait EpgmBaseLayoutFactory extends BaseLayoutFactory[G, V, E] {

  override def graphHeadEncoder: Encoder[EpgmGraphHead] = EpgmGraphHead.encoder

  override def vertexEncoder: Encoder[EpgmVertex] = EpgmVertex.encoder

  override def edgeEncoder: Encoder[EpgmEdge] = EpgmEdge.encoder

  override def graphHeadFactory: GraphHeadFactory[G] = EpgmGraphHead

  override def vertexFactory: VertexFactory[V] = EpgmVertex

  override def edgeFactory: EdgeFactory[E] = EpgmEdge
}
