package org.gradoop.spark.io.impl.csv

import org.gradoop.common.model.api.elements._
import org.gradoop.common.model.impl.id.GradoopId
import org.gradoop.common.properties.PropertyValue
import org.gradoop.spark.io.impl.csv.CsvConstants.ParseFunction
import org.gradoop.spark.util.StringEscaper

abstract protected class CsvParser[G <: GraphHead, V <: Vertex, E <: Edge]
(var metadata: Option[MetaData], elementFactoryProvider: ElementFactoryProvider[G, V, E]) extends Serializable {

  def graphHeadParseFunctions: Array[ParseFunction[G]]

  def vertexParseFunctions: Array[ParseFunction[V]]

  def edgeParseFunctions: Array[ParseFunction[E]]

  // Parse functions

  protected def parseNothing[EL <: Element](element: Option[EL], string: String): Option[EL] = element

  protected def createGraphHead(graphHead: Option[G], idString: String): Option[G] = {
    val id = GradoopId.fromString(idString)
    graphHead match {
      case None => Some(elementFactoryProvider.graphHeadFactory(id))
      case Some(g) =>
        g.id_=(id)
        Some(g)
    }
  }

  protected def createVertex(vertex: Option[V], idString: String): Option[V] = {
    val id = GradoopId.fromString(idString)
    vertex match {
      case None => Some(elementFactoryProvider.vertexFactory(id))
      case Some(v) =>
        v.id = id
        Some(v)
    }
  }

  protected def createEdge(edge: Option[E], idString: String): Option[E] = {
    val id = GradoopId.fromString(idString)
    edge match {
      case None => Some(elementFactoryProvider.edgeFactory(id))
      case Some(e) =>
        e.id_=(id)
        Some(e)
    }
  }

  protected def parseGraphIds[GE <: GraphElement](element: Option[GE], idSetString: String): Option[GE] = {
    val idSet: IdSet = idSetString.substring(1, idSetString.length - 1)
      .split(CsvConstants.LIST_DELIMITER)
      .map(GradoopId.fromString).toSet
    element.map(e => {
      e.graphIds = idSet
      e
    })
  }

  protected def parseSourceId(edge: Option[E], sourceIdString: String): Option[E] = {
    val id: Id = GradoopId.fromString(sourceIdString)
    edge.map(e => {
      e.sourceId = id
      e
    })
  }

  protected def parseTargetId(edge: Option[E], targetIdString: String): Option[E] = {
    val id: Id = GradoopId.fromString(targetIdString)
    edge.map(e => {
      e.targetId = id
      e
    })
  }

  protected def parseLabels[EL <: Element](element: Option[EL], labelsString: String): Option[EL] = {
    val labels: Labels = StringEscaper.unescape(labelsString)//StringEscaper.split(labelsString, GradoopConstants.LABEL_DELIMITER)
      //.map(StringEscaper.unescape)
    element.map(e => {
      e.labels = labels
      e
    })
  }

  protected def parseProperties[EL <: Element](element: Option[EL], propertiesString: String): Option[EL] = {
    element.map(e => {
      e.properties = Map[String, PV]("all" -> PropertyValue(propertiesString))
      e
    })
  } // TODO
}
