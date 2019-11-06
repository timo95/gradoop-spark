package org.gradoop.spark.io.impl.csv

import org.gradoop.common.model.api.elements._
import org.gradoop.spark.io.impl.csv.CsvConstants.ComposeFunction
import org.gradoop.spark.util.StringEscaper

abstract class CsvComposer[G <: GraphHead, V <: Vertex, E <: Edge](var metadata: Option[MetaData])
  extends Serializable {

  def getGraphHeadComposeFunctions: Array[ComposeFunction[G]]

  def getVertexComposeFunctions: Array[ComposeFunction[V]]

  def getEdgeComposeFunctions: Array[ComposeFunction[E]]

  // Compose functions

  def composeId[T <: Identifiable](obj: T): String = {
    obj.getId.toString
  }

  def composeGraphIds[GE <: GraphElement](element: GE): String = {
    element.getGraphIds.mkString("[", CsvConstants.LIST_DELIMITER, "]")
  }

  def composeSourceId(edge: E): String = {
    edge.getSourceId.toString
  }

  def composeTargetId(edge: E): String = {
    edge.getTargetId.toString
  }

  def composeLabels[T <: Labeled](obj: T): String = {
    StringEscaper.escape(obj.getLabels, CsvConstants.ESCAPED_CHARS)//obj.getLabels.map(label => StringEscaper.escape(label, CsvConstants.ESCAPED_CHARS))
      //.mkString(CsvConstants.LIST_DELIMITER)
  }

  def composeProperties[T <: Attributed](obj: T): String = {
    obj.getProperties("all").getString //obj.getProperties // TODO
  }
}
