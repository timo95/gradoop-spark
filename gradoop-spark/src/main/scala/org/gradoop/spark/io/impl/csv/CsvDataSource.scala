package org.gradoop.spark.io.impl.csv

import org.apache.spark.sql.{Dataset, Row}
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.common.model.impl.id.GradoopId
import org.gradoop.spark.io.api.DataSource
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}

class CsvDataSource[
  G <: GraphHead,
  V <: Vertex,
  E <: Edge,
  LG <: LogicalGraph[G, V, E, LG, GC],
  GC <: GraphCollection[G, V, E, LG, GC]]
(csvPath: String, config: GradoopSparkConfig[G, V, E, LG, GC], metadata: Option[MetaData])
  extends CsvParser[G, V, E](metadata) with DataSource {
  import config.implicits._

  private val options: Map[String, String] = Map(
    "sep" -> CsvConstants.TOKEN_DELIMITER,
    "quote" -> null) // default is '"' but we don't support quoting and don't escape quotes

  override def readLogicalGraph: LG = config.logicalGraphFactory.init(readGraphHeads, readVertices, readEdges)

  override def readGraphCollection: GC = config.graphCollectionFactory.init(readGraphHeads, readVertices, readEdges)

  def readGraphHeads: Dataset[G] = {
    config.sparkSession.read
      .options(options)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.GRAPH_HEAD_FILE)
      .map(rowToGraphHead)
  }

  def readVertices: Dataset[V] = {
    config.sparkSession.read
      .options(options)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.VERTEX_FILE)
      .map(rowToVertex)
  }

  def readEdges: Dataset[E] = {
    config.sparkSession.read
      .options(options)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.EDGE_FILE)
      .map(rowToEdge)
  }

  def rowToGraphHead(row: Row): G = {
    config.logicalGraphFactory.graphHeadFactory(
      parseId(row.getString(0)),
      parseLabels(row.getString(1)),
      parseProperties(row.getString(2)))
  }

  def rowToVertex(row: Row): V = {
    config.logicalGraphFactory.vertexFactory(
      parseId(row.getString(0)),
      parseLabels(row.getString(2)),
      parseProperties(row.getString(3)),
      parseGraphIds(row.getString(1)))
  }

  def rowToEdge(row: Row): E = {
    try {
      config.logicalGraphFactory.edgeFactory(
        parseId(row.getString(0)),
        parseLabels(row.getString(4)),
        parseId(row.getString(2)), // sourceId
        parseId(row.getString(3)), // targetId
        parseProperties(row.getString(5)),
        parseGraphIds(row.getString(1)))
    } catch {
      case _: NullPointerException => println(row)
        config.logicalGraphFactory.edgeFactory.create(GradoopId.get, GradoopId.get)
    }
  }
}

object CsvDataSource {

  def apply[
    G <: GraphHead,
    V <: Vertex,
    E <: Edge,
    LG <: LogicalGraph[G, V, E, LG, GC],
    GC <: GraphCollection[G, V, E, LG, GC]]
  (csvPath: String, config: GradoopSparkConfig[G, V, E, LG, GC]): CsvDataSource[G, V, E, LG, GC] = {
    new CsvDataSource(csvPath, config, None)
  }

  def apply[
    G <: GraphHead,
    V <: Vertex,
    E <: Edge,
    LG <: LogicalGraph[G, V, E, LG, GC],
    GC <: GraphCollection[G, V, E, LG, GC]]
  (csvPath: String, config: GradoopSparkConfig[G, V, E, LG, GC], metaData: MetaData): CsvDataSource[G, V, E, LG, GC] = {
    new CsvDataSource(csvPath, config, Some(metaData))
  }
}