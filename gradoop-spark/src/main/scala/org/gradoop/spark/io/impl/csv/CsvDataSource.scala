package org.gradoop.spark.io.impl.csv

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.io.api.DataSource
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}

class CsvDataSource[
  G <: GraphHead,
  V <: Vertex,
  E <: Edge,
  LG <: LogicalGraph[G, V, E, LG, GC],
  GC <: GraphCollection[G, V, E, LG, GC]]
(csvPath: String, config: GradoopSparkConfig[G, V, E, LG, GC], csvParser: CsvParser[G, V, E]) extends DataSource {

  override def getLogicalGraph: LG = config.getLogicalGraphFactory.init(getGraphHeads, getVertices, getEdges)

  override def getGraphCollection: GC = config.getGraphCollectionFactory.init(getGraphHeads, getVertices, getEdges)

  def getGraphHeads: Dataset[G] = {
    config.getSparkSession.read
      .option("delimiter", CsvConstants.TOKEN_DELIMITER)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.GRAPH_HEAD_FILE)
      .flatMap(new RowToObject[G](csvParser.getGraphHeadParseFunctions).call)(config.getGraphHeadEncoder)
  }

  def getVertices: Dataset[V] = {
    config.getSparkSession.read
      .option("delimiter", CsvConstants.TOKEN_DELIMITER)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.VERTEX_FILE)
      .flatMap(new RowToObject[V](csvParser.getVertexParseFunctions).call)(config.getVertexEncoder)
  }

  def getEdges: Dataset[E] = {
    config.getSparkSession.read
      .option("delimiter", CsvConstants.TOKEN_DELIMITER)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.EDGE_FILE)
      .flatMap(new RowToObject[E](csvParser.getEdgeParseFunctions).call)(config.getEdgeEncoder)
  }
}
