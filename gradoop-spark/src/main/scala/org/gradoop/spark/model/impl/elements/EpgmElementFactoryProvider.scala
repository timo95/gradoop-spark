package org.gradoop.spark.model.impl.elements

import org.gradoop.common.model.api.elements.{EdgeFactory, ElementFactoryProvider, GraphHeadFactory, VertexFactory}

trait EpgmElementFactoryProvider extends ElementFactoryProvider[G, V, E] {

  override def getGraphHeadFactory: GraphHeadFactory[G] = EpgmGraphHead

  override def getVertexFactory: VertexFactory[V] = EpgmVertex

  override def getEdgeFactory: EdgeFactory[E] = EpgmEdge
}

object EpgmElementFactoryProvider extends EpgmElementFactoryProvider