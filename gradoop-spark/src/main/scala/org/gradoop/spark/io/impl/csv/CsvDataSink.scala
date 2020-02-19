package org.gradoop.spark.io.impl.csv

import org.apache.spark.sql.SaveMode
import org.gradoop.spark.io.api.DataSink
import org.gradoop.spark.io.impl.metadata.MetaData
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.types.Gve

class CsvDataSink[L <: Gve[L]] private (csvPath: String, config: GradoopSparkConfig[L], metaDataOpt: Option[MetaData])
  extends CsvDataSinkBase(csvPath) with DataSink[L] {
  import config.Implicits._

  override def write(logicalGraph: L#LG, saveMode: SaveMode): Unit = {
    val metaData = metaDataOpt.getOrElse(MetaData(logicalGraph))
    writeGraphHeads(CsvConstants.GRAPH_HEAD_FILE, logicalGraph.graphHead.toDF, metaData.graphHeadMetaData, saveMode)
    writeVertices(CsvConstants.VERTEX_FILE, logicalGraph.vertices.toDF, metaData.vertexMetaData, saveMode)
    writeEdges(CsvConstants.EDGE_FILE, logicalGraph.edges.toDF, metaData.edgeMetaData, saveMode)
    CsvMetaDataSink(csvPath).write(metaData, saveMode)
  }

  override def write(graphCollection: L#GC, saveMode: SaveMode): Unit = {
    val metaData = metaDataOpt.getOrElse(MetaData(graphCollection))
    writeGraphHeads(CsvConstants.GRAPH_HEAD_FILE, graphCollection.graphHeads.toDF, metaData.graphHeadMetaData, saveMode)
    writeVertices(CsvConstants.VERTEX_FILE, graphCollection.vertices.toDF, metaData.vertexMetaData, saveMode)
    writeEdges(CsvConstants.EDGE_FILE, graphCollection.edges.toDF, metaData.edgeMetaData, saveMode)
    CsvMetaDataSink(csvPath).write(metaData, saveMode)
  }
}

object CsvDataSink {

  def apply[L <: Gve[L]](csvPath: String, config: GradoopSparkConfig[L]): CsvDataSink[L] = {
    new CsvDataSink(csvPath, config, None)
  }

  def apply[L <: Gve[L]](csvPath: String, config: GradoopSparkConfig[L], metadata: MetaData): CsvDataSink[L] = {
    new CsvDataSink(csvPath, config, Some(metadata))
  }
}
