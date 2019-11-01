package org.gradoop.spark.io.impl.csv.epgm

import org.gradoop.spark.io.impl.csv.CsvConstants.ParseFunction
import org.gradoop.spark.io.impl.csv.{CsvParser, MetaData}
import org.gradoop.spark.model.impl.elements.EpgmElementFactoryProvider

class EpgmCsvParser(metadata: Option[MetaData])
  extends CsvParser[G, V, E](metadata, EpgmElementFactoryProvider) {

  def getGraphHeadParseFunctions: Array[ParseFunction[G]] = {
    Array[ParseFunction[G]](parseGraphHeadId, parseLabels, parseProperties)
  }

  def getVertexParseFunctions: Array[ParseFunction[V]] = {
    Array[ParseFunction[V]](parseVertexId, parseGraphIds, parseLabels, parseProperties)
  }

  def getEdgeParseFunctions: Array[ParseFunction[E]] = {
    Array[ParseFunction[E]](parseEdgeId, parseGraphIds, parseSourceId, parseTargetId, parseLabels, parseProperties)
  }
}
