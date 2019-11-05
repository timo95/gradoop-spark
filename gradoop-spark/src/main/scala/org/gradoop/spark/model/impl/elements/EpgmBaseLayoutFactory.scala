package org.gradoop.spark.model.impl.elements

import org.apache.spark.sql.Encoder
import org.gradoop.common.model.api.elements.{EdgeFactory, GraphHeadFactory, VertexFactory}
import org.gradoop.spark.model.api.layouts.BaseLayoutFactory

trait EpgmBaseLayoutFactory extends BaseLayoutFactory[G, V, E] {

  override def getGraphHeadEncoder: Encoder[EpgmGraphHead] = EpgmGraphHead.getEncoder

  override def getVertexEncoder: Encoder[EpgmVertex] = EpgmVertex.getEncoder

  override def getEdgeEncoder: Encoder[EpgmEdge] = EpgmEdge.getEncoder

  override def getGraphHeadFactory: GraphHeadFactory[G] = EpgmGraphHead

  override def getVertexFactory: VertexFactory[V] = EpgmVertex

  override def getEdgeFactory: EdgeFactory[E] = EpgmEdge
}
