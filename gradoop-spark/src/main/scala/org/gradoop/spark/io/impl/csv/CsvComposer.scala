package org.gradoop.spark.io.impl.csv

import org.apache.spark.broadcast.Broadcast
import org.gradoop.common.model.api.components._
import org.gradoop.spark.io.impl.csv.CsvConstants.ComposeFunction
import org.gradoop.spark.io.impl.metadata.ElementMetaData
import org.gradoop.spark.model.impl.types.Gve
import org.gradoop.spark.util.StringEscaper

object CsvComposer extends Serializable {

  // Compose functions

  def composeId[A <: Identifiable](obj: A): String = obj.id.toString

  def composeGraphIds[GE <: Contained](element: GE): String = {
    element.graphIds.mkString("[", CsvConstants.LIST_DELIMITER, "]")
  }

  def composeSourceId[A <: Edge](edge: A): String = edge.sourceId.toString

  def composeTargetId[A <: Edge](edge: A): String = edge.targetId.toString

  def composeLabels[A <: Labeled](obj: A): String = {
    StringEscaper.escape(obj.label, CsvConstants.ESCAPED_CHARS)
  }

  def composeProperties[A <: Attributed](obj: A): String = {
    //metaData.value.metaData

    obj.properties.values
      .map(p => StringEscaper.escape(p.toString, CsvConstants.ESCAPED_CHARS))
      .mkString(CsvConstants.VALUE_DELIMITER) // TODO metadata
  }
}
