package org.gradoop.spark.io.impl.csv.indexed

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.expressions.FilterExpressions
import org.gradoop.spark.io.api.DataSource
import org.gradoop.spark.io.impl.csv.CsvConstants._
import org.gradoop.spark.io.impl.csv.CsvDataSourceBase
import org.gradoop.spark.io.impl.metadata.{ElementMetaData, PropertyMetaData}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.types.Tfl
import org.gradoop.spark.util.{StringEscaper, TflFunctions}

class IndexedCsvDataSource[L <: Tfl[L]](csvPath: String, config: GradoopSparkConfig[L])
  extends CsvDataSourceBase(csvPath) with DataSource[L] {
  private val factory = config.logicalGraphFactory
  import factory.Implicits._
  implicit val sparkSession = config.sparkSession

  override def readLogicalGraph: L#LG = {
    val (graphHeads, graphHeadProp) = TflFunctions.splitGraphHeadMap(getMetaData.graphHeadMetaData.collect.map(m =>
      (m.label, readGraphHeads(indexedCsvPath(GRAPH_HEAD_PATH, m.label), parseProperties(_, m)))).toMap)
    val (vertices, vertexProp) = TflFunctions.splitVertexMap(getMetaData.vertexMetaData.collect.map(m =>
      (m.label, readVertices(indexedCsvPath(VERTEX_PATH, m.label), parseProperties(_, m)))).toMap)
    val (edges, edgeProp) = TflFunctions.splitEdgeMap(getMetaData.edgeMetaData.collect.map(m =>
      (m.label, readEdges(indexedCsvPath(EDGE_PATH, m.label), parseProperties(_, m)))).toMap)

    config.logicalGraphFactory.init(graphHeads, vertices, edges, graphHeadProp, vertexProp, edgeProp)
  }

  override def readGraphCollection: L#GC = {
    val (graphHeads, graphHeadProp) = TflFunctions.splitGraphHeadMap(getMetaData.graphHeadMetaData.collect.map(m =>
      (m.label, readGraphHeads(indexedCsvPath(GRAPH_HEAD_PATH, m.label), parseProperties(_, m)))).toMap)
    val (vertices, vertexProp) = TflFunctions.splitVertexMap(getMetaData.vertexMetaData.collect.map(m =>
      (m.label, readVertices(indexedCsvPath(VERTEX_PATH, m.label), parseProperties(_, m)))).toMap)
    val (edges, edgeProp) = TflFunctions.splitEdgeMap(getMetaData.edgeMetaData.collect.map(m =>
      (m.label, readEdges(indexedCsvPath(EDGE_PATH, m.label), parseProperties(_, m)))).toMap)

    config.graphCollectionFactory.init(graphHeads, vertices, edges, graphHeadProp, vertexProp, edgeProp)
  }

  private def parseProperties(dataFrame: DataFrame, metaData: ElementMetaData): DataFrame = {
    dataFrame.filter(FilterExpressions.hasLabel(metaData.label))
      .filter(FilterExpressions.hasLabel(metaData.label)) // cleanFilename can result in reading duplicates
      .withColumn(ColumnNames.LABEL, lit(metaData.label)) // make label constant
      .withColumn(ColumnNames.PROPERTIES,
        map_from_entries(parseProperties(col(ColumnNames.PROPERTIES), lit(metaData.label),
          typedLit[Seq[PropertyMetaData]](metaData.metaData))))
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
