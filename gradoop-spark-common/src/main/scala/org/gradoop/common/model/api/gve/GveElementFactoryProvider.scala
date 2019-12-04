package org.gradoop.common.model.api.gve

/**
 * Trait that provides getters for the element factories.
 */
trait GveElementFactoryProvider[G <: GveGraphHead, V <: GveVertex, E <: GveEdge] extends Serializable {

  /** Get the factory that is responsible for creating graph head instances.
   *
   * @return a factory that creates graph heads
   */
  def graphHeadFactory: GveGraphHeadFactoryGve[G]

  /** Get the factory that is responsible for creating vertex instances.
   *
   * @return a factory that creates vertices
   */
  def vertexFactory: GveVertexFactory[V]

  /** Get the factory that is responsible for creating edge instances.
   *
   * @return a factory that creates edges
   */
  def edgeFactory: GveEdgeFactory[E]
}
