package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Encoder
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}

trait ElementEncoderProvider[G <: GraphHead, V <: Vertex, E <: Edge] {

  def graphHeadEncoder: Encoder[G]

  def vertexEncoder: Encoder[V]

  def edgeEncoder: Encoder[E]
}
