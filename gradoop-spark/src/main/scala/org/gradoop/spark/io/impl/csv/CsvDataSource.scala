package org.gradoop.spark.io.impl.csv

import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.io.api.DataSource
import org.gradoop.spark.io.impl.metadata.{ElementMetaData, MetaData}
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
class CsvDataSource[L <: Gve[L]](csvPath: String, config: GradoopSparkConfig[L]) extends DataSource[L] {
  import config.Implicits._
  private val factory = config.logicalGraphFactory
  import factory.Implicits._

  private val options: Map[String, String] = Map(
    "sep" -> CsvConstants.TOKEN_DELIMITER,
    "quote" -> null) // default is '"' but we don't support quoting and don't escape quotes

  private val parseId = udf(s => CsvParser.parseId(s))
  private val parseLabel = udf(s => CsvParser.parseLabel(s))
  private val parseGraphIds = udf(s => CsvParser.parseGraphIds(s))
  private val parseProperties = udf((s, label, metaData) =>
    CsvParser.parseProperties(s, label, metaData))

  private val parserPropertiesExpression = map_from_entries(parseProperties(col(ColumnNames.PROPERTIES),
    col(ColumnNames.LABEL), col("metaData")))

  private var metaData: Option[MetaData] = None

  override def readLogicalGraph: L#LG = {
    val meta = getMetaData
    config.logicalGraphFactory.init(readGraphHeads(meta.graphHeadMetaData),
      readVertices(meta.vertexMetaData),
      readEdges(meta.edgeMetaData))
  }

  override def readGraphCollection: L#GC = {
    val meta = getMetaData
    config.graphCollectionFactory.init(readGraphHeads(meta.graphHeadMetaData),
      readVertices(meta.vertexMetaData),
      readEdges(meta.edgeMetaData))
  }

  def getMetaData: MetaData = {
    if(metaData.isEmpty) metaData = Some(new CsvMetaDataSource(csvPath).read)
    metaData.get
  }

  def readGraphHeads(metaData: Dataset[ElementMetaData]): Dataset[L#G] = {
    // read file
    val strings = config.sparkSession.read
      .options(options)
      .schema(StructType(Seq(
        StructField(ColumnNames.ID, DataTypes.StringType, false),
        StructField(ColumnNames.LABEL, DataTypes.StringType, true),
        StructField(ColumnNames.PROPERTIES, DataTypes.StringType, true)
      )))
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.GRAPH_HEAD_FILE)

    // parse id and label
    val partiallyParsed = strings.select(
      parseId(col(ColumnNames.ID)).as(ColumnNames.ID),
      parseLabel(col(ColumnNames.LABEL)).as(ColumnNames.LABEL),
      col(ColumnNames.PROPERTIES)
    )

    // parse properties
    partiallyParsed.join(metaData, ColumnNames.LABEL)
      .withColumn(ColumnNames.PROPERTIES, parserPropertiesExpression.as(ColumnNames.PROPERTIES))
      .drop("metaData")
      .as[L#G]
  }

  def readVertices(metaData: Dataset[ElementMetaData]): Dataset[L#V] = {
    // read file
    val strings = config.sparkSession.read
      .options(options)
      .schema(StructType(Seq(
        StructField(ColumnNames.ID, DataTypes.StringType, false),
        StructField(ColumnNames.GRAPH_IDS, DataTypes.StringType, false),
        StructField(ColumnNames.LABEL, DataTypes.StringType, true),
        StructField(ColumnNames.PROPERTIES, DataTypes.StringType, true)
      )))
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.VERTEX_FILE)

    // parse ids and label
    val partiallyParsed = strings.select(
      parseId(col(ColumnNames.ID)).as(ColumnNames.ID),
      parseGraphIds(col(ColumnNames.GRAPH_IDS)).as(ColumnNames.GRAPH_IDS),
      parseLabel(col(ColumnNames.LABEL)).as(ColumnNames.LABEL),
      col(ColumnNames.PROPERTIES)
    )

    // parse properties
    partiallyParsed.join(metaData, ColumnNames.LABEL)
      .withColumn(ColumnNames.PROPERTIES, parserPropertiesExpression.as(ColumnNames.PROPERTIES))
      .drop("metaData")
      .as[L#V]
  }

  def readEdges(metaData: Dataset[ElementMetaData]): Dataset[L#E] = {
    // read file
    val strings = config.sparkSession.read
      .options(options)
      .schema(StructType(Seq(
        StructField(ColumnNames.ID, DataTypes.StringType, false),
        StructField(ColumnNames.GRAPH_IDS, DataTypes.StringType, false),
        StructField(ColumnNames.SOURCE_ID, DataTypes.StringType, false),
        StructField(ColumnNames.TARGET_ID, DataTypes.StringType, false),
        StructField(ColumnNames.LABEL, DataTypes.StringType, true),
        StructField(ColumnNames.PROPERTIES, DataTypes.StringType, true)
      )))
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.EDGE_FILE)

    // parse ids and label
    val partiallyParsed = strings.select(
      parseId(col(ColumnNames.ID)).as(ColumnNames.ID),
      parseGraphIds(col(ColumnNames.GRAPH_IDS)).as(ColumnNames.GRAPH_IDS),
      parseId(col(ColumnNames.SOURCE_ID)).as(ColumnNames.SOURCE_ID),
      parseId(col(ColumnNames.TARGET_ID)).as(ColumnNames.TARGET_ID),
      parseLabel(col(ColumnNames.LABEL)).as(ColumnNames.LABEL),
      col(ColumnNames.PROPERTIES)
    )

    // parse properties
    partiallyParsed.join(metaData, ColumnNames.LABEL)
      .withColumn(ColumnNames.PROPERTIES, parserPropertiesExpression.as(ColumnNames.PROPERTIES))
      .drop("metaData")
      .as[L#E]
  }
}

object CsvDataSource {

  def apply[L <: Gve[L]](csvPath: String, config: GradoopSparkConfig[L]): CsvDataSource[L] = {
    new CsvDataSource(csvPath, config)
  }
}
