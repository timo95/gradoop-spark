package org.gradoop.spark.io.impl.csv

import org.gradoop.common.model.api.elements._
import org.gradoop.common.model.impl.id.GradoopId
import org.gradoop.common.util.GradoopConstants
import org.gradoop.spark.io.impl.csv.CsvConstants.ParseFunction
import org.gradoop.spark.util.StringEscaper

abstract protected class CsvParser[G <: GraphHead, V <: Vertex, E <: Edge]
(var metadata: Option[MetaData], elementFactoryProvider: ElementFactoryProvider[G, V, E]) extends Serializable {

  def getGraphHeadParseFunctions: Array[ParseFunction[G]]

  def getVertexParseFunctions: Array[ParseFunction[V]]

  def getEdgeParseFunctions: Array[ParseFunction[E]]

  // Parse functions

  protected def parseNothing[EL <: Element](element: Option[EL], string: String): Option[EL] = element

  protected def createGraphHead(graphHead: Option[G], idString: String): Option[G] = {
    val id = GradoopId.fromString(idString)
    graphHead match {
      case None => Some(elementFactoryProvider.getGraphHeadFactory(id))
      case Some(g) =>
        g.setId(id)
        Some(g)
    }
  }

  protected def createVertex(vertex: Option[V], idString: String): Option[V] = {
    val id = GradoopId.fromString(idString)
    vertex match {
      case None => Some(elementFactoryProvider.getVertexFactory(id))
      case Some(v) =>
        v.setId(id)
        Some(v)
    }
  }

  protected def createEdge(edge: Option[E], idString: String): Option[E] = {
    val id = GradoopId.fromString(idString)
    edge match {
      case None => Some(elementFactoryProvider.getEdgeFactory(id))
      case Some(e) =>
        e.setId(id)
        Some(e)
    }
  }

  protected def parseGraphIds[GE <: GraphElement](element: Option[GE], idSetString: String): Option[GE] = {
    val idSet: IdSet = idSetString.substring(1, idSetString.length - 1)
      .split(CsvConstants.LIST_DELIMITER)
      .map(GradoopId.fromString).toSet
    element.map(e => {
      e.setGraphIds(idSet)
      e
    })
  }

  protected def parseSourceId(edge: Option[E], sourceIdString: String): Option[E] = {
    val id: Id = GradoopId.fromString(sourceIdString)
    edge.map(e => {
      e.setSourceId(id)
      e
    })
  }

  protected def parseTargetId(edge: Option[E], targetIdString: String): Option[E] = {
    val id: Id = GradoopId.fromString(targetIdString)
    edge.map(e => {
      e.setTargetId(id)
      e
    })
  }

  protected def parseLabels[EL <: Element](element: Option[EL], labelsString: String): Option[EL] = {
    val labels: Labels = StringEscaper.split(labelsString, GradoopConstants.LABEL_DELIMITER)
      .map(StringEscaper.unescape)
    element.map(e => {
      e.setLabels(labels)
      e
    })
  }

  protected def parseProperties[EL <: Element](element: Option[EL], propertiesString: String): Option[EL] = {
    element.map(e => {
      e.setProperties(propertiesString)
      e
    })
  } // TODO
}
