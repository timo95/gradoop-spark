package org.gradoop.spark.model.api.layouts

import org.gradoop.spark.model.api.elements.{Edge, EdgeFactory, GraphHead, GraphHeadFactory, Vertex, VertexFactory}
import org.gradoop.spark.model.api.graph.{ElementFactoryProvider, GraphCollection, LogicalGraph}

abstract class BaseLayoutFactory[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph, GC <: GraphCollection] extends ElementFactoryProvider[G, V, E] {

  var graphHeadFactory: GraphHeadFactory[G]
  var vertexFactory: VertexFactory[V]
  var edgeFactory: EdgeFactory[E]

  /**
   * Get the factory that is responsible for creating graph head instances.
   *
   * @return a factory that creates graph heads
   */
  override def getGraphHeadFactory: GraphHeadFactory[G] = graphHeadFactory

  /**
   * Get the factory that is responsible for creating vertex instances.
   *
   * @return a factory that creates vertices
   */
  override def getVertexFactory: VertexFactory[V] = vertexFactory

  /**
   * Get the factory that is responsible for creating edge instances.
   *
   * @return a factory that creates edges
   */
  override def getEdgeFactory: EdgeFactory[E] = edgeFactory
}
