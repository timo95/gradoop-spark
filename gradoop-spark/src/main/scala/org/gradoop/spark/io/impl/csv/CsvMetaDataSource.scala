package org.gradoop.spark.io.impl.csv

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.gradoop.spark.io.api.MetaDataSource
import org.gradoop.spark.io.impl.metadata.{ElementMetaData, MetaData, PropertyMetaData}
import org.gradoop.spark.util.StringEscaper

class CsvMetaDataSource(csvPath: String)(implicit sparkSession: SparkSession)
  extends MetaDataSource {

  private val options: Map[String, String] = Map(
    "sep" -> CsvConstants.TOKEN_DELIMITER,
    "quote" -> null) // default is '"' but we don't support quoting and don't escape quotes

  private val TYPE_FIELD = "type"

  private val schema = StructType(Seq(
    StructField(TYPE_FIELD, DataTypes.StringType, false),
    StructField(ElementMetaData.label, DataTypes.StringType, true),
    StructField(ElementMetaData.metaData, DataTypes.StringType, true)))

  override def read: MetaData = {
    val dataFrame = sparkSession.read
      .options(options)
      .schema(schema)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.METADATA_FILE)

    new MetaData(getElementMetaData(dataFrame, CsvConstants.GRAPH_TYPE),
      getElementMetaData(dataFrame, CsvConstants.VERTEX_TYPE),
      getElementMetaData(dataFrame, CsvConstants.EDGE_TYPE))
  }

  private def getElementMetaData(dataFrame: DataFrame, elementType: String): Dataset[ElementMetaData] = {
    import sparkSession.implicits._
    import org.apache.spark.sql.functions._

    dataFrame
      .filter(col(TYPE_FIELD) === lit(elementType))
      .select(ElementMetaData.label, ElementMetaData.metaData)
      .map(rowToElementMetaData)
  }

  private def rowToElementMetaData(row: Row): ElementMetaData = {
    val label: String = if(row.getString(0) == null) "" else StringEscaper.unescape(row.getString(0))
    val propertyMetaData =
      if(row.getString(1) == null) Array.empty[PropertyMetaData]
      else {
        StringEscaper.split(row.getString(1), CsvConstants.LIST_DELIMITER)
          .map(string => StringEscaper.split(string, CsvConstants.PROPERTY_TOKEN_DELIMITER, 2))
          .map(array => PropertyMetaData(StringEscaper.unescape(array(0)), array(1).toLowerCase))
      }

    ElementMetaData(label, propertyMetaData)
  }
}

object CsvMetaDataSource {

  def apply(csvPath: String)(implicit session: SparkSession): CsvMetaDataSource = {
    new CsvMetaDataSource(csvPath)
  }
}
