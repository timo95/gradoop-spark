package org.gradoop.spark.io.impl.csv

import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{Dataset, Row, SaveMode, SparkSession}
import org.gradoop.common.util.GradoopConstants
import org.gradoop.spark.io.api.DataSink
import org.gradoop.spark.io.impl.csv.CsvConstants.ComposeFunction
import org.gradoop.spark.io.impl.metadata.{ElementMetaData, MetaData}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.types.Gve

class CsvDataSink[L <: Gve[L]] private (csvPath: String, config: GradoopSparkConfig[L], getMetaData: => MetaData)
  extends DataSink[L] {
  import config.Implicits._

  private val options: Map[String, String] = Map(
    "sep" -> CsvConstants.TOKEN_DELIMITER,
    "nullValue" -> GradoopConstants.NULL_STRING,
    "escapeQuotes" -> "false",
    "emptyValue" -> "")

  override def write(logicalGraph: L#LG, saveMode: SaveMode): Unit = {
    val metaData = getMetaData
    writeGraphHeads(logicalGraph.graphHead, metaData.graphHeadMetaData, saveMode)
    writeVertices(logicalGraph.vertices, metaData.vertexMetaData, saveMode)
    writeEdges(logicalGraph.edges, metaData.edgeMetaData, saveMode)
    new CsvMetaDataSink(csvPath).write(metaData, saveMode)
  }

  override def write(graphCollection: L#GC, saveMode: SaveMode): Unit = {
    val metaData = getMetaData
    writeGraphHeads(graphCollection.graphHeads, metaData.graphHeadMetaData, saveMode)
    writeVertices(graphCollection.vertices, metaData.vertexMetaData, saveMode)
    writeEdges(graphCollection.edges, metaData.edgeMetaData, saveMode)
    new CsvMetaDataSink(csvPath).write(metaData, saveMode)
  }

  def writeGraphHeads(graphHeads: Dataset[L#G], metaData: Dataset[ElementMetaData], saveMode: SaveMode): Unit = {
    implicit val rowEncoder: ExpressionEncoder[Row] = RowEncoder(StructType( // TODO metadata
      graphHeadComposeFunctions.indices
        .map(i => StructField(i.toString, DataTypes.StringType, nullable = false))))

    graphHeads.map(g => Row.fromSeq(graphHeadComposeFunctions.map(_(g))))
      .write
      .options(options)
      .mode(saveMode)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.GRAPH_HEAD_FILE)
  }

  def writeVertices(vertices: Dataset[L#V], metaData: Dataset[ElementMetaData], saveMode: SaveMode): Unit = {
    implicit val rowEncoder: ExpressionEncoder[Row] = RowEncoder(StructType(
      vertexComposeFunctions.indices
        .map(i => StructField(i.toString, DataTypes.StringType, nullable = false))))

    vertices.map(g => Row.fromSeq(vertexComposeFunctions.map(_(g))))
      .write
      .options(options)
      .mode(saveMode)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.VERTEX_FILE)
  }

  def writeEdges(edges: Dataset[L#E], metaData: Dataset[ElementMetaData], saveMode: SaveMode): Unit = {
    implicit val rowEncoder: ExpressionEncoder[Row] = RowEncoder(StructType(
      edgeComposeFunctions.indices
        .map(i => StructField(i.toString, DataTypes.StringType, nullable = false))))

    edges.map(g => Row.fromSeq(edgeComposeFunctions.map(_(g))))
      .write
      .options(options)
      .mode(saveMode)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.EDGE_FILE)
  }

  def graphHeadComposeFunctions: Array[ComposeFunction[L#G]] = {
    Array(CsvComposer.composeId, CsvComposer.composeLabels, CsvComposer.composeProperties)
  }

  def vertexComposeFunctions: Array[ComposeFunction[L#V]] = {
    Array(CsvComposer.composeId, CsvComposer.composeGraphIds, CsvComposer.composeLabels, CsvComposer.composeProperties)
  }

  def edgeComposeFunctions: Array[ComposeFunction[L#E]] = {
    Array(CsvComposer.composeId, CsvComposer.composeGraphIds, CsvComposer.composeSourceId, CsvComposer.composeTargetId,
      CsvComposer.composeLabels, CsvComposer.composeProperties)
  }
}

object CsvDataSink {

  def apply[L <: Gve[L]](csvPath: String, config: GradoopSparkConfig[L]): CsvDataSink[L] = {
    new CsvDataSink(csvPath, config, new CsvMetaDataSource(csvPath)(config.sparkSession).read)
  }

  def apply[L <: Gve[L]](csvPath: String, config: GradoopSparkConfig[L], metadata: MetaData): CsvDataSink[L] = {
    new CsvDataSink(csvPath, config, metadata)
  }
}
