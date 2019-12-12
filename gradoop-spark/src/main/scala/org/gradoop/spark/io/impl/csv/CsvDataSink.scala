package org.gradoop.spark.io.impl.csv

import org.apache.spark.sql.{Dataset, SaveMode, SparkSession}
import org.gradoop.common.util.GradoopConstants
import org.gradoop.spark.io.api.DataSink
import org.gradoop.spark.io.impl.csv.CsvConstants.ComposeFunction
import org.gradoop.spark.io.impl.metadata.MetaData
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.types.Gve

class CsvDataSink[L <: Gve[L]](csvPath: String, config: GradoopSparkConfig[L], metadata: Option[MetaData])
  extends CsvComposer[L](metadata) with DataSink[L] {
  implicit val session: SparkSession = config.sparkSession
  import config.Implicits._

  private val options: Map[String, String] = Map(
    "sep" -> CsvConstants.TOKEN_DELIMITER,
    "nullValue" -> GradoopConstants.NULL_STRING,
    "escapeQuotes" -> "false",
    "emptyValue" -> "")

  override def write(logicalGraph: L#LG, saveMode: SaveMode): Unit = {
    writeGraphHeads(logicalGraph.graphHead, saveMode)
    writeVertices(logicalGraph.vertices, saveMode)
    writeEdges(logicalGraph.edges, saveMode)
  }

  override def write(graphCollection: L#GC, saveMode: SaveMode): Unit = {
    writeGraphHeads(graphCollection.graphHeads, saveMode)
    writeVertices(graphCollection.vertices, saveMode)
    writeEdges(graphCollection.edges, saveMode)
  }

  def writeGraphHeads(graphHeads: Dataset[L#G], saveMode: SaveMode): Unit = {
    val objectToRow = new ObjectToRow[L#G](graphHeadComposeFunctions)
    graphHeads.map(objectToRow.call)(objectToRow.encoder)
      .write
      .options(options)
      .mode(saveMode)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.GRAPH_HEAD_FILE)
  }

  def writeVertices(vertices: Dataset[L#V], saveMode: SaveMode): Unit = {
    val objectToRow = new ObjectToRow[L#V](vertexComposeFunctions)
    vertices.map(objectToRow.call)(objectToRow.encoder)
      .write
      .options(options)
      .mode(saveMode)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.VERTEX_FILE)
  }

  def writeEdges(edges: Dataset[L#E], saveMode: SaveMode): Unit = {
    val objectToRow = new ObjectToRow[L#E](edgeComposeFunctions)
    edges.map(objectToRow.call)(objectToRow.encoder)
      .write
      .options(options)
      .mode(saveMode)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.EDGE_FILE)
  }

  def graphHeadComposeFunctions: Array[ComposeFunction[L#G]] = {
    Array(composeId, composeLabels, composeProperties)
  }

  def vertexComposeFunctions: Array[ComposeFunction[L#V]] = {
    Array(composeId, composeGraphIds, composeLabels, composeProperties)
  }

  def edgeComposeFunctions: Array[ComposeFunction[L#E]] = {
    Array(composeId, composeGraphIds, composeSourceId, composeTargetId, composeLabels, composeProperties)
  }
}

object CsvDataSink {

  def apply[L <: Gve[L]]
  (csvPath: String, config: GradoopSparkConfig[L]): CsvDataSink[L] = {
    new CsvDataSink(csvPath, config, None)
  }

  def apply[L <: Gve[L]]
  (csvPath: String, config: GradoopSparkConfig[L], metadata: MetaData): CsvDataSink[L] = {
    new CsvDataSink(csvPath, config, Some(metadata))
  }
}
