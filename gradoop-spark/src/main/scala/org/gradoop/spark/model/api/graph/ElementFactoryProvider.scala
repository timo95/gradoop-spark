package org.gradoop.spark.model.api.graph

import org.gradoop.common.model.api.elements.{EdgeFactory, GraphHeadFactory, VertexFactory}
import org.gradoop.spark.model.api.elements.{GraphHeadFactory, VertexFactory}

/**
 * Trait that provides getters for the element factories.
 */
trait ElementFactoryProvider[G, V, E] {
  var graphHeadFactory: GraphHeadFactory[G]
  var vertexFactory: VertexFactory[V]
  var edgeFactory: EdgeFactory[E]

  /**
   * Get the factory that is responsible for creating graph head instances.
   *
   * @return a factory that creates graph heads
   */
  def getGraphHeadFactory: GraphHeadFactory[G] = graphHeadFactory

  /**
   * Get the factory that is responsible for creating vertex instances.
   *
   * @return a factory that creates vertices
   */
  def getVertexFactory: VertexFactory[V] = vertexFactory

  /**
   * Get the factory that is responsible for creating edge instances.
   *
   * @return a factory that creates edges
   */
  def getEdgeFactory: EdgeFactory[E] = edgeFactory
}
