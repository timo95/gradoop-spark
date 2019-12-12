package org.gradoop.spark.io.impl.csv

import org.apache.spark.sql.{Dataset, Row}
import org.gradoop.common.properties.PropertyValue
import org.gradoop.spark.io.api.DataSource
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.types.Gve

class CsvDataSource[L <: Gve[L]](csvPath: String, config: GradoopSparkConfig[L])
  extends CsvParser[L] with DataSource[L] {
  import config.Implicits._
  private val factory = config.logicalGraphFactory
  import factory.Implicits._

  private val options: Map[String, String] = Map(
    "sep" -> CsvConstants.TOKEN_DELIMITER,
    "quote" -> null) // default is '"' but we don't support quoting and don't escape quotes

  private val metaData = new CsvMetaDataSource(csvPath).read

  private val graphHeadMetaData = metaData.graphHeadMetaData.collect()
  private val vertexMetaData = metaData.vertexMetaData.collect() // TODO: property parsing with sql - join metadata and transform
  private val edgeMetaData = metaData.edgeMetaData.collect()

  override def readLogicalGraph: L#LG = {
    config.logicalGraphFactory.init(readGraphHeads, readVertices, readEdges)
  }

  override def readGraphCollection: L#GC = {
    config.graphCollectionFactory.init(readGraphHeads, readVertices, readEdges)
  }

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
    val label = parseLabel(row.getString(1))
    val elementMetaData = graphHeadMetaData.filter(m => m.label == label)
    config.logicalGraphFactory.graphHeadFactory(
      parseId(row.getString(0)),
      label,
      if(elementMetaData.isEmpty) Map.empty[String, PropertyValue]
      else parseProperties(row.getString(2), elementMetaData(0)))
  }

  def rowToVertex(row: Row): L#V = {
    val label = parseLabel(row.getString(2))
    val elementMetaData = vertexMetaData.filter(m => m.label == label)
    config.logicalGraphFactory.vertexFactory(
      parseId(row.getString(0)),
      label,
      if(elementMetaData.isEmpty) Map.empty[String, PropertyValue]
      else parseProperties(row.getString(3), elementMetaData(0)), parseGraphIds(row.getString(1)))
  }

  def rowToEdge(row: Row): L#E = {
    val label = parseLabel(row.getString(4))
    val elementMetaData = edgeMetaData.filter(m => m.label == label)
    config.logicalGraphFactory.edgeFactory(
      parseId(row.getString(0)),
      label,
      parseId(row.getString(2)), // sourceId
      parseId(row.getString(3)), // targetId
      if(elementMetaData.isEmpty) Map.empty[String, PropertyValue]
      else parseProperties(row.getString(5), elementMetaData(0)), parseGraphIds(row.getString(1)))
  }
}

object CsvDataSource {

  def apply[L <: Gve[L]](csvPath: String, config: GradoopSparkConfig[L]): CsvDataSource[L] = new CsvDataSource(csvPath, config)
}
