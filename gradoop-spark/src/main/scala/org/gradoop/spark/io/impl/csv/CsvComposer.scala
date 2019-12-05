package org.gradoop.spark.io.impl.csv

import org.gradoop.common.model.api.components._
import org.gradoop.spark.io.impl.csv.CsvConstants.ComposeFunction
import org.gradoop.spark.io.impl.metadata.MetaData
import org.gradoop.spark.model.impl.types.Gve
import org.gradoop.spark.util.StringEscaper

abstract class CsvComposer[L <: Gve[L]](var metadata: Option[MetaData]) extends Serializable {

  def graphHeadComposeFunctions: Array[ComposeFunction[L#G]]

  def vertexComposeFunctions: Array[ComposeFunction[L#V]]

  def edgeComposeFunctions: Array[ComposeFunction[L#E]]

  // Compose functions

  def composeId[T <: Identifiable](obj: T): String = obj.id.toString

  def composeGraphIds[GE <: Contained](element: GE): String = {
    element.graphIds.mkString("[", CsvConstants.LIST_DELIMITER, "]")
  }

  def composeSourceId(edge: L#E): String = edge.sourceId.toString

  def composeTargetId(edge: L#E): String = edge.targetId.toString

  def composeLabels[T <: Labeled](obj: T): String = {
    StringEscaper.escape(obj.label, CsvConstants.ESCAPED_CHARS)
    //obj.getLabels.map(label => StringEscaper.escape(label, CsvConstants.ESCAPED_CHARS))
      //.mkString(CsvConstants.LIST_DELIMITER)
  }

  def composeProperties[T <: Attributed](obj: T): String = {
    obj.properties.values
      .map(p => StringEscaper.escape(p.getString, CsvConstants.ESCAPED_CHARS))
      .mkString(CsvConstants.VALUE_DELIMITER) // TODO metadata
  }
}
