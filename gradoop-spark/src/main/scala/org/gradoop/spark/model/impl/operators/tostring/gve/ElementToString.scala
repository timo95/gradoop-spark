package org.gradoop.spark.model.impl.operators.tostring.gve

import org.gradoop.common.model.api.elements.AttributedElement
import org.gradoop.common.model.api.gve.{GveEdge, GveGraphHead, GveVertex}

object ElementToString {

  // Empty String
  def graphHeadToEmptyString[G <: GveGraphHead](graphHead: G): GraphHeadString = {
    GraphHeadString(graphHead.id, "")
  }

  // Id String
  def graphHeadToIdString[G <: GveGraphHead](graphHead: G): GraphHeadString = {
    GraphHeadString(graphHead.id, graphHead.id.toString)
  }

  def vertexToIdString[V <: GveVertex](vertex: V): TraversableOnce[VertexString] = {
    val vertexString = "(%s)".format(vertex.id)
    vertex.graphIds.map(graphId => VertexString(graphId, vertex.id, vertexString))
  }

  def edgeToIdString[E <: GveEdge](edge: E): TraversableOnce[EdgeString] = {
    val edgeString = "[%s]".format(edge.id)
    edge.graphIds.map(graphId => EdgeString(graphId, edge.sourceId, edge.targetId, "", edgeString, ""))
  }

  // Data String
  def graphHeadToDataString[G <: GveGraphHead](graphHead: G): GraphHeadString = {
    GraphHeadString(graphHead.id, "|%s|".format(labelWithProperties(graphHead)))
  }

  def vertexToDataString[V <: GveVertex](vertex: V): TraversableOnce[VertexString] = {
    val vertexString = "(%s)".format(labelWithProperties(vertex))
    vertex.graphIds.map(graphId => VertexString(graphId, vertex.id, vertexString))
  }

  def edgeToDataString[E <: GveEdge](edge: E): TraversableOnce[EdgeString] = {
    val edgeString = "[%s]".format(labelWithProperties(edge))
    edge.graphIds.map(graphId => EdgeString(graphId, edge.sourceId, edge.targetId, "", edgeString, ""))
  }


  // Helper functions
  private def labelWithProperties(element: AttributedElement): String = {
    val label = if (element.label == null) "" else element.label
    label + "{" + element.properties.map(entry => entry._1 + "=" + entry._2).toSeq.sorted.mkString(",") + "}"
  }
}
