package org.gradoop.spark.io.impl.csv

import org.apache.spark.sql.{Dataset, Row}
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.io.api.DataSource
import org.gradoop.spark.io.impl.metadata.MetaData
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}
import org.gradoop.spark.model.impl.types.GveLayoutType

class CsvDataSource[L <: GveLayoutType]
(csvPath: String, config: GradoopSparkConfig[L], metadata: Option[MetaData])
  extends CsvParser[L](metadata) with DataSource[L] {
  import config.implicits._

  private val options: Map[String, String] = Map(
    "sep" -> CsvConstants.TOKEN_DELIMITER,
    "quote" -> null) // default is '"' but we don't support quoting and don't escape quotes

  override def readLogicalGraph: LogicalGraph[L] = config.logicalGraphFactory.init(readGraphHeads, readVertices, readEdges)

  override def readGraphCollection: GraphCollection[L] = config.graphCollectionFactory.init(readGraphHeads, readVertices, readEdges)

  def readGraphHeads: Dataset[L#G] = {
    config.sparkSession.read
      .options(options)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.GRAPH_HEAD_FILE)
      .map(rowToGraphHead)
  }

  def readVertices: Dataset[L#V] = {
    config.sparkSession.read
      .options(options)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.VERTEX_FILE)
      .map(rowToVertex)
  }

  def readEdges: Dataset[L#E] = {
    config.sparkSession.read
      .options(options)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.EDGE_FILE)
      .map(rowToEdge)
  }

  def rowToGraphHead(row: Row): L#G = {
    config.logicalGraphFactory.graphHeadFactory(
      parseId(row.getString(0)),
      parseLabels(row.getString(1)),
      parseProperties(row.getString(2)))
  }

  def rowToVertex(row: Row): L#V = {
    config.logicalGraphFactory.vertexFactory(
      parseId(row.getString(0)),
      parseLabels(row.getString(2)),
      parseProperties(row.getString(3)),
      parseGraphIds(row.getString(1)))
  }

  def rowToEdge(row: Row): L#E = {
    config.logicalGraphFactory.edgeFactory(
      parseId(row.getString(0)),
      parseLabels(row.getString(4)),
      parseId(row.getString(2)), // sourceId
      parseId(row.getString(3)), // targetId
      parseProperties(row.getString(5)),
      parseGraphIds(row.getString(1)))
  }
}

object CsvDataSource {

  def apply[L <: GveLayoutType]
  (csvPath: String, config: GradoopSparkConfig[L]): CsvDataSource[L] = {
    new CsvDataSource(csvPath, config, None)
  }

  def apply[L <: GveLayoutType]
  (csvPath: String, config: GradoopSparkConfig[L], metaData: MetaData): CsvDataSource[L] = {
    new CsvDataSource(csvPath, config, Some(metaData))
  }
}