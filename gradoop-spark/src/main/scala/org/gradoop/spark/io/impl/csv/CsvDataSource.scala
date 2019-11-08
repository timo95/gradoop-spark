package org.gradoop.spark.io.impl.csv

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.common.util.GradoopConstants
import org.gradoop.spark.io.api.DataSource
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}

abstract class CsvDataSource[
  G <: GraphHead,
  V <: Vertex,
  E <: Edge,
  LG <: LogicalGraph[G, V, E, LG, GC],
  GC <: GraphCollection[G, V, E, LG, GC]]
(csvPath: String, config: GradoopSparkConfig[G, V, E, LG, GC], metadata: Option[MetaData])
  extends CsvParser[G, V, E](metadata, config.logicalGraphFactory) with DataSource {

  private val options: Map[String, String] = Map(
    "sep" -> CsvConstants.TOKEN_DELIMITER,
    "quote" -> null,
    "nullValue" -> GradoopConstants.NULL_STRING)

  override def readLogicalGraph: LG = config.logicalGraphFactory.init(readGraphHeads, readVertices, readEdges)

  override def readGraphCollection: GC = config.graphCollectionFactory.init(readGraphHeads, readVertices, readEdges)

  def readGraphHeads: Dataset[G] = {
    config.getSparkSession.read
      .options(options)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.GRAPH_HEAD_FILE)
      .flatMap(new RowToObject[G](graphHeadParseFunctions).call)(config.graphHeadEncoder)
  }

  def readVertices: Dataset[V] = {
    config.getSparkSession.read
      .options(options)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.VERTEX_FILE)
      .flatMap(new RowToObject[V](vertexParseFunctions).call)(config.vertexEncoder)
  }

  def readEdges: Dataset[E] = {
    config.getSparkSession.read
      .options(options)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.EDGE_FILE)
      .flatMap(new RowToObject[E](edgeParseFunctions).call)(config.edgeEncoder)
  }
}
