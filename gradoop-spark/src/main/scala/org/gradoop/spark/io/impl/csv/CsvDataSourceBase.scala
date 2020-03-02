package org.gradoop.spark.io.impl.csv

import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.io.impl.metadata.MetaData

class CsvDataSourceBase(csvPath: String) {

  // Csv options
  private val OPTIONS: Map[String, String] = Map(
    "sep" -> CsvConstants.TOKEN_DELIMITER,
    "quote" -> null) // default is '"' but we don't support quoting and don't escape quotes

  // UDFs for parsing csv fields
  private val parseId = udf(s => CsvParser.parseId(s))
  private val parseLabel = udf(s => CsvParser.parseLabel(s))
  private val parseGraphIds = udf(s => CsvParser.parseGraphIds(s))
  protected val parseProperties = udf((s, label, metaData) =>
    CsvParser.parseProperties(s, label, metaData))

  // Metadata
  private var metaData: Option[MetaData] = None

  protected def getMetaData(implicit sparkSession: SparkSession): MetaData = {
    if(metaData.isEmpty) metaData = Some(new CsvMetaDataSource(csvPath).read)
    metaData.get
  }

  /** Read graph head csv file
   *
   * @param localPath local path in csv path
   * @param sparkSession spark session
   * @return dataframe that contains all gve graphhead fields
   */
  protected def readGraphHeads(localPath: String, parseProperties: DataFrame => DataFrame)
    (implicit sparkSession: SparkSession): DataFrame = {
    // read file
    val strings = sparkSession.read
      .options(OPTIONS)
      .schema(StructType(Seq(
        StructField(ColumnNames.ID, DataTypes.StringType, nullable = false),
        StructField(ColumnNames.LABEL, DataTypes.StringType, nullable = true),
        StructField(ColumnNames.PROPERTIES, DataTypes.StringType, nullable = true)
      )))
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + localPath)

    // parse id and label
    val partiallyParsed = strings.select(
      parseId(col(ColumnNames.ID)).as(ColumnNames.ID),
      parseLabel(col(ColumnNames.LABEL)).as(ColumnNames.LABEL),
      col(ColumnNames.PROPERTIES)
    )

    parseProperties(partiallyParsed)
  }

  /** Read vertex csv file
   *
   * @param localPath local path in csv path
   * @param sparkSession spark session
   * @return dataframe that contains all gve vertex fields
   */
  protected def readVertices(localPath: String, parseProperties: DataFrame => DataFrame)
    (implicit sparkSession: SparkSession): DataFrame = {
    // read file
    val strings = sparkSession.read
      .options(OPTIONS)
      .schema(StructType(Seq(
        StructField(ColumnNames.ID, DataTypes.StringType, nullable = false),
        StructField(ColumnNames.GRAPH_IDS, DataTypes.StringType, nullable = false),
        StructField(ColumnNames.LABEL, DataTypes.StringType, nullable = true),
        StructField(ColumnNames.PROPERTIES, DataTypes.StringType, nullable = true)
      )))
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + localPath)

    // parse ids and label
    val partiallyParsed = strings.select(
      parseId(col(ColumnNames.ID)).as(ColumnNames.ID),
      parseGraphIds(col(ColumnNames.GRAPH_IDS)).as(ColumnNames.GRAPH_IDS),
      parseLabel(col(ColumnNames.LABEL)).as(ColumnNames.LABEL),
      col(ColumnNames.PROPERTIES)
    )

    parseProperties(partiallyParsed)
  }

  /** Read edge csv file
   *
   * @param localPath local path in csv path
   * @param sparkSession spark session
   * @return dataframe that contains all gve edge fields
   */
  protected def readEdges(localPath: String, parseProperties: DataFrame => DataFrame)
    (implicit sparkSession: SparkSession): DataFrame = {
    // read file
    val strings = sparkSession.read
      .options(OPTIONS)
      .schema(StructType(Seq(
        StructField(ColumnNames.ID, DataTypes.StringType, nullable = false),
        StructField(ColumnNames.GRAPH_IDS, DataTypes.StringType, nullable = false),
        StructField(ColumnNames.SOURCE_ID, DataTypes.StringType, nullable = false),
        StructField(ColumnNames.TARGET_ID, DataTypes.StringType, nullable = false),
        StructField(ColumnNames.LABEL, DataTypes.StringType, nullable = true),
        StructField(ColumnNames.PROPERTIES, DataTypes.StringType, nullable = true)
      )))
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + localPath)

    // parse ids and label
    val partiallyParsed = strings.select(
      parseId(col(ColumnNames.ID)).as(ColumnNames.ID),
      parseGraphIds(col(ColumnNames.GRAPH_IDS)).as(ColumnNames.GRAPH_IDS),
      parseId(col(ColumnNames.SOURCE_ID)).as(ColumnNames.SOURCE_ID),
      parseId(col(ColumnNames.TARGET_ID)).as(ColumnNames.TARGET_ID),
      parseLabel(col(ColumnNames.LABEL)).as(ColumnNames.LABEL),
      col(ColumnNames.PROPERTIES)
    )

    parseProperties(partiallyParsed)
  }
}
