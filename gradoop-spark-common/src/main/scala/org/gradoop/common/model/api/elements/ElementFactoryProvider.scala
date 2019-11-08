package org.gradoop.common.model.api.elements

/**
 * Trait that provides getters for the element factories.
 */
trait ElementFactoryProvider[G <: GraphHead, V <: Vertex, E <: Edge] extends Serializable {

  /** Get the factory that is responsible for creating graph head instances.
   *
   * @return a factory that creates graph heads
   */
  def graphHeadFactory: GraphHeadFactory[G]

  /** Get the factory that is responsible for creating vertex instances.
   *
   * @return a factory that creates vertices
   */
  def vertexFactory: VertexFactory[V]

  /** Get the factory that is responsible for creating edge instances.
   *
   * @return a factory that creates edges
   */
  def edgeFactory: EdgeFactory[E]
}
