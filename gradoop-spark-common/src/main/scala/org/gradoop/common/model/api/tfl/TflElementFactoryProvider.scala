package org.gradoop.common.model.api.tfl

trait TflElementFactoryProvider[G <: TflGraphHead, V <: TflVertex, E <: TflEdge, P <: TflProperties]
  extends Serializable {

  /** Get the factory that is responsible for creating graph head instances.
   *
   * @return a factory that creates graph heads
   */
  def graphHeadFactory: TflGraphHeadFactory[G]

  /** Get the factory that is responsible for creating vertex instances.
   *
   * @return a factory that creates vertices
   */
  def vertexFactory: TflVertexFactory[V]

  /** Get the factory that is responsible for creating edge instances.
   *
   * @return a factory that creates edges
   */
  def edgeFactory: TflEdgeFactory[E]

  /** Get the factory that is responsible for creating properties instances.
   *
   * @return a factory that creates properties
   */
  def propertiesFactory: TflPropertiesFactory[P]
}
