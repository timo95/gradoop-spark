package org.gradoop.spark.model.impl.operators.tostring.gve

import org.gradoop.common.model.api.elements.AttributedElement
import org.gradoop.common.model.api.gve.{GveEdge, GveGraphHead, GveVertex}

object ElementToString {

  private def labelWithProperties(element: AttributedElement): String = {
    if (element.label == null) ""
    else "{" + element.properties.map(_.toString).toSeq.sorted.mkString(",") + "}"
  }

  def graphHeadToDataString[G <: GveGraphHead](graphHead: G): GraphHeadString = {
    GraphHeadString(graphHead.id, "|" + labelWithProperties(graphHead) + "|")
  }

  def vertexToDataString[V <: GveVertex](vertex: V): TraversableOnce[VertexString] = {
    val vertexString = "(" + labelWithProperties(vertex) + ")"
    vertex.graphIds.map(graphId => VertexString(graphId, vertex.id, vertexString))
  }

  def edgeToDataString[E <: GveEdge](edge: E): TraversableOnce[EdgeString] = {
    val edgeString = "[" + labelWithProperties(edge) + "]"
    edge.graphIds.map(graphId => EdgeString(graphId, edge.sourceId, edge.targetId, "", edgeString, ""))
  }
}
