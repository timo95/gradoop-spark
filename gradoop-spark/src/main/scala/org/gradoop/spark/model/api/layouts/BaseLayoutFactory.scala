package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Encoder
import org.gradoop.common.model.api.elements.{Edge, ElementFactoryProvider, GraphHead, Vertex}

trait BaseLayoutFactory[G <: GraphHead, V <: Vertex, E <: Edge] extends ElementFactoryProvider[G, V, E] {

  def getGraphHeadEncoder: Encoder[G]

  def getVertexEncoder: Encoder[V]

  def getEdgeEncoder: Encoder[E]
}
