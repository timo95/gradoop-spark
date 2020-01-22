package org.gradoop.spark.io.impl.csv

import org.apache.spark.sql._
import org.gradoop.spark.io.api.MetaDataSink
import org.gradoop.spark.io.impl.metadata.{ElementMetaData, MetaData}
import org.gradoop.spark.util.StringEscaper

class CsvMetaDataSink(csvPath: String)(implicit sparkSession: SparkSession) extends MetaDataSink {

  private val options: Map[String, String] = Map(
    "sep" -> CsvConstants.TOKEN_DELIMITER,
    "quote" -> null) // default is '"' but we don't support quoting and don't escape quotes

  override def write(metaData: MetaData, saveMode: SaveMode): Unit = {
    val stringRows = getRows(CsvConstants.GRAPH_TYPE, metaData.graphHeadMetaData).union(
      getRows(CsvConstants.VERTEX_TYPE, metaData.vertexMetaData)).union(
      getRows(CsvConstants.EDGE_TYPE, metaData.edgeMetaData))

    stringRows
      .write
      .options(options)
      .mode(saveMode)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.METADATA_FILE)
  }

  private def getRows(elementType: String, metaData: Dataset[ElementMetaData]): DataFrame = {
    import org.apache.spark.sql.functions._

    val composeLabel = udf((l: String) => StringEscaper.escape(l, CsvConstants.ESCAPED_CHARS))
    val composeMetaData = udf((meta: Seq[Row]) => {
      meta.map(m => StringEscaper.escape(m(0).asInstanceOf[String], CsvConstants.ESCAPED_CHARS) +
        CsvConstants.PROPERTY_TOKEN_DELIMITER + m(1))
        .mkString(CsvConstants.LIST_DELIMITER)
    })

    metaData.select(lit(elementType),
      composeLabel(col(ElementMetaData.label)),
      composeMetaData(col(ElementMetaData.metaData)))
  }
}

object CsvMetaDataSink {

  def apply(csvPath: String)(implicit sparkSession: SparkSession): CsvMetaDataSink = {
    new CsvMetaDataSink(csvPath)
  }
}
