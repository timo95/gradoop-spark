package org.gradoop.spark.io.impl.csv

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Dataset, SaveMode}
import org.gradoop.common.model.api.components.Attributed
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.io.api.DataSink
import org.gradoop.spark.io.impl.metadata.{ElementMetaData, MetaData}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.types.Gve

class CsvDataSink[L <: Gve[L]] private (csvPath: String, config: GradoopSparkConfig[L], metaDataOpt: Option[MetaData])
  extends CsvDataSinkBase(csvPath) with DataSink[L] {
  import config.Implicits._

  override def write(logicalGraph: L#LG, saveMode: SaveMode): Unit = {
    val metaData = metaDataOpt.getOrElse(MetaData(logicalGraph))
    writeGraphHeads(CsvConstants.GRAPH_HEAD_FILE, propertiesToStr(logicalGraph.graphHeads, metaData.graphHeadMetaData), saveMode)
    writeVertices(CsvConstants.VERTEX_FILE, propertiesToStr(logicalGraph.vertices, metaData.vertexMetaData), saveMode)
    writeEdges(CsvConstants.EDGE_FILE, propertiesToStr(logicalGraph.edges, metaData.edgeMetaData), saveMode)
    CsvMetaDataSink(csvPath).write(metaData, saveMode)
  }

  override def write(graphCollection: L#GC, saveMode: SaveMode): Unit = {
    val metaData = metaDataOpt.getOrElse(MetaData(graphCollection))
    writeGraphHeads(CsvConstants.GRAPH_HEAD_FILE, propertiesToStr(graphCollection.graphHeads, metaData.graphHeadMetaData), saveMode)
    writeVertices(CsvConstants.VERTEX_FILE, propertiesToStr(graphCollection.vertices, metaData.vertexMetaData), saveMode)
    writeEdges(CsvConstants.EDGE_FILE, propertiesToStr(graphCollection.edges, metaData.edgeMetaData), saveMode)
    CsvMetaDataSink(csvPath).write(metaData, saveMode)
  }

  private def propertiesToStr[T <: Attributed](dataset: Dataset[T], metaData: Dataset[ElementMetaData]): DataFrame = {
    dataset.join(metaData, ColumnNames.LABEL)
      .withColumn(ColumnNames.PROPERTIES,
        propertiesToStrUdf(col(ColumnNames.PROPERTIES), col(ElementMetaData.metaData)))
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
