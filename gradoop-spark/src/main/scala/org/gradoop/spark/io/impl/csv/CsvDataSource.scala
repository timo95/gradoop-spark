package org.gradoop.spark.io.impl.csv

import org.apache.spark.sql.functions.{col, map_from_entries}
import org.apache.spark.sql.{Column, DataFrame, Dataset}
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.io.api.DataSource
import org.gradoop.spark.io.impl.csv.CsvConstants.{EDGE_FILE, GRAPH_HEAD_FILE, VERTEX_FILE}
import org.gradoop.spark.io.impl.metadata.ElementMetaData
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.types.Gve

/**
 * Read Graph from CSV file.
 *
 * Any elements or properties not included in the metadata are not read.
 *
 * @param csvPath Path to read from
 * @param config Gradoop config
 * @tparam L Layout type
 */
class CsvDataSource[L <: Gve[L]](csvPath: String, config: GradoopSparkConfig[L])
  extends CsvDataSourceBase(csvPath) with DataSource[L] {
  import config.Implicits._
  private val factory = config.logicalGraphFactory
  import factory.Implicits._

  // Column expressions for parsing csv fields
  private val parserPropertiesExpression: Column = map_from_entries(parseProperties(col(ColumnNames.PROPERTIES),
    col(ColumnNames.LABEL), col(ElementMetaData.metaData)))

  override def readLogicalGraph: L#LG = {
    val meta = getMetaData
    config.logicalGraphFactory.init(
      readGraphHeads(GRAPH_HEAD_FILE, parseProperties(_, meta.graphHeadMetaData)).as[L#G],
      readVertices(VERTEX_FILE, parseProperties(_, meta.vertexMetaData)).as[L#V],
      readEdges(EDGE_FILE, parseProperties(_, meta.edgeMetaData)).as[L#E])
  }

  override def readGraphCollection: L#GC = {
    val meta = getMetaData
    config.graphCollectionFactory.init(
      readGraphHeads(GRAPH_HEAD_FILE, parseProperties(_, meta.graphHeadMetaData)).as[L#G],
      readVertices(VERTEX_FILE, parseProperties(_, meta.vertexMetaData)).as[L#V],
      readEdges(EDGE_FILE, parseProperties(_, meta.edgeMetaData)).as[L#E])
  }

  private def parseProperties(dataFrame: DataFrame, metaData: Dataset[ElementMetaData]): DataFrame = {
    dataFrame.join(metaData, ColumnNames.LABEL)
      .withColumn(ColumnNames.PROPERTIES, parserPropertiesExpression.as(ColumnNames.PROPERTIES))
      .drop(ElementMetaData.metaData)
  }
}

object CsvDataSource {

  def apply[L <: Gve[L]](csvPath: String, config: GradoopSparkConfig[L]): CsvDataSource[L] = {
    new CsvDataSource(csvPath, config)
  }
}
