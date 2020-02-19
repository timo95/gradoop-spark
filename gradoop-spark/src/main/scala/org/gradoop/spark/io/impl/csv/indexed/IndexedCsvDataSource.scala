package org.gradoop.spark.io.impl.csv.indexed

import org.gradoop.spark.expressions.FilterExpressions
import org.gradoop.spark.io.api.DataSource
import org.gradoop.spark.io.impl.csv.CsvConstants._
import org.gradoop.spark.io.impl.csv.CsvDataSourceBase
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.types.Tfl
import org.gradoop.spark.util.{StringEscaper, TflFunctions}

class IndexedCsvDataSource[L <: Tfl[L]](csvPath: String, config: GradoopSparkConfig[L])
  extends CsvDataSourceBase(csvPath) with DataSource[L] {
  private val factory = config.logicalGraphFactory
  import factory.Implicits._
  implicit val sparkSession = config.sparkSession
  import sparkSession.implicits._

  override def readLogicalGraph: L#LG = {
    val (graphHeads, graphHeadProp) = TflFunctions.splitGraphHeadMap(getMetaData.graphHeadLabels.collect.map(l =>
      (l, readGraphHeads(indexedCsvPath(GRAPH_HEAD_PATH, l), getMetaData.graphHeadMetaData(l))
        .filter(FilterExpressions.hasLabel(l)))).toMap) // filter vs duplicates on label->directory collisions
    val (vertices, vertexProp) = TflFunctions.splitVertexMap(getMetaData.vertexLabels.collect.map(l =>
      (l, readVertices(indexedCsvPath(VERTEX_PATH, l), getMetaData.vertexMetaData(l))
        .filter(FilterExpressions.hasLabel(l)))).toMap)
    val (edges, edgeProp) = TflFunctions.splitEdgeMap(getMetaData.edgeLabels.collect.map(l =>
      (l, readEdges(indexedCsvPath(EDGE_PATH, l), getMetaData.edgeMetaData(l))
        .filter(FilterExpressions.hasLabel(l)))).toMap)

    config.logicalGraphFactory.init(graphHeads, vertices, edges, graphHeadProp, vertexProp, edgeProp)
  }

  override def readGraphCollection: L#GC = {
    val (graphHeads, graphHeadProp) = TflFunctions.splitGraphHeadMap(getMetaData.graphHeadLabels.collect.map(l =>
      (l, readGraphHeads(indexedCsvPath(GRAPH_HEAD_PATH, l), getMetaData.graphHeadMetaData(l))
        .filter(FilterExpressions.hasLabel(l)))).toMap)
    val (vertices, vertexProp) = TflFunctions.splitVertexMap(getMetaData.vertexLabels.collect.map(l =>
      (l, readVertices(indexedCsvPath(VERTEX_PATH, l), getMetaData.vertexMetaData(l))
        .filter(FilterExpressions.hasLabel(l)))).toMap)
    val (edges, edgeProp) = TflFunctions.splitEdgeMap(getMetaData.edgeLabels.collect.map(l =>
      (l, readEdges(indexedCsvPath(EDGE_PATH, l), getMetaData.edgeMetaData(l))
        .filter(FilterExpressions.hasLabel(l)))).toMap)

    config.graphCollectionFactory.init(graphHeads, vertices, edges, graphHeadProp, vertexProp, edgeProp)
  }

  private def indexedCsvPath(elementPath: String, label: String): String = {
    val dir = if (label.isEmpty) DEFAULT_DIRECTORY.toString
    else cleanFilename(StringEscaper.escape(label, ESCAPED_CHARS))
    elementPath + DIRECTORY_SEPARATOR + dir + DIRECTORY_SEPARATOR + SIMPLE_FILE
  }

  private def cleanFilename(filename: String): String = filename.replaceAll("[<>:\"/\\\\|?*]", "_").toLowerCase
}

object IndexedCsvDataSource {

  def apply[L <: Tfl[L]](csvPath: String, config: GradoopSparkConfig[L]): IndexedCsvDataSource[L] = {
    new IndexedCsvDataSource(csvPath, config)
  }
}
