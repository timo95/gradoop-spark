package org.gradoop.spark.io.impl.csv

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.common.util.GradoopConstants
import org.gradoop.spark.io.api.DataSink
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}

abstract class CsvDataSink[
  G <: GraphHead,
  V <: Vertex,
  E <: Edge,
  LG <: LogicalGraph[G, V, E, LG, GC],
  GC <: GraphCollection[G, V, E, LG, GC]]
(csvPath: String, config: GradoopSparkConfig[G, V, E, LG, GC], metadata: Option[MetaData])
extends CsvComposer[G, V, E](metadata) with DataSink[G, V, E, LG, GC] {
  implicit val session: SparkSession = config.getSparkSession

  private val options: Map[String, String] = Map(
    "sep" -> CsvConstants.TOKEN_DELIMITER,
    "nullValue" -> GradoopConstants.NULL_STRING,
    "escapeQuotes" -> "false",
    "emptyValue" -> "")

  override def write(logicalGraph: LG): Unit = {
    write(logicalGraph, SaveMode.ErrorIfExists)
  }

  override def write(logicalGraph: LG, saveMode: SaveMode): Unit = {
    writeGraphHeads(logicalGraph.getGraphHead, saveMode)
    writeVertices(logicalGraph.getVertices, saveMode)
    writeEdges(logicalGraph.getEdges, saveMode)
  }

  override def write(graphCollection: GC): Unit = {
    write(graphCollection, SaveMode.ErrorIfExists)
  }

  override def write(graphCollection: GC, saveMode: SaveMode): Unit = {
    writeGraphHeads(graphCollection.getGraphHeads, saveMode)
    writeVertices(graphCollection.getVertices, saveMode)
    writeEdges(graphCollection.getEdges, saveMode)
  }

  def writeGraphHeads(graphHeads: Dataset[G], saveMode: SaveMode): Unit = {
    val objectToRow = new ObjectToRow[G](getGraphHeadComposeFunctions)
    graphHeads.map(objectToRow.call)(objectToRow.getEncoder)
      .write
      .options(options)
      .mode(saveMode)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.GRAPH_HEAD_FILE)
  }

  def writeVertices(vertices: Dataset[V], saveMode: SaveMode): Unit = {
    val objectToRow = new ObjectToRow[V](getVertexComposeFunctions)
    vertices.map(objectToRow.call)(objectToRow.getEncoder)
      .write
      .options(options)
      .mode(saveMode)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.VERTEX_FILE)
  }

  def writeEdges(edges: Dataset[E], saveMode: SaveMode): Unit = {
    val objectToRow = new ObjectToRow[E](getEdgeComposeFunctions)
    edges.map(objectToRow.call)(objectToRow.getEncoder)
      .write
      .options(options)
      .mode(saveMode)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.EDGE_FILE)
  }
}
