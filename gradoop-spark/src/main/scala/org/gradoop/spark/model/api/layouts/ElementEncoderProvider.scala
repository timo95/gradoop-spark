package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Encoder
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}

trait ElementEncoderProvider[G <: GraphHead, V <: Vertex, E <: Edge] {

  def getGraphHeadEncoder: Encoder[G]

  def getVertexEncoder: Encoder[V]

  def getEdgeEncoder: Encoder[E]
}
