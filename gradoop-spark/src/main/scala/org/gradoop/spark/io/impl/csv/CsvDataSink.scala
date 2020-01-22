package org.gradoop.spark.io.impl.csv

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Dataset, Row, SaveMode}
import org.gradoop.common.id.GradoopId
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.{ColumnNames, GradoopConstants}
import org.gradoop.spark.io.api.DataSink
import org.gradoop.spark.io.impl.metadata.{ElementMetaData, MetaData}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.types.Gve
import org.gradoop.spark.util.StringEscaper

class CsvDataSink[L <: Gve[L]] private (csvPath: String, config: GradoopSparkConfig[L], metaDataOpt: Option[MetaData])
  extends DataSink[L] {

  private val options: Map[String, String] = Map(
    "sep" -> CsvConstants.TOKEN_DELIMITER,
    "nullValue" -> GradoopConstants.NULL_STRING,
    "escapeQuotes" -> "false",
    "emptyValue" -> "")

  private val idToStr = udf((id: Row) => new GradoopId(id(0).asInstanceOf[Array[Byte]]).toString)
  private val graphIdsToStr = udf((graphIds: Seq[Row]) => graphIds
    .map(id => new GradoopId(id(0).asInstanceOf[Array[Byte]]).toString)
    .mkString("[", CsvConstants.LIST_DELIMITER, "]"))
  private val labelToStr = udf(label => StringEscaper.escape(label, CsvConstants.ESCAPED_CHARS))
  private val propertiesToStr = udf((properties: Map[String, Row], metaData: Seq[Row]) =>
    CsvDataSink.propertiesToString(properties, metaData))

  import config.Implicits._

  override def write(logicalGraph: L#LG, saveMode: SaveMode): Unit = {
    val metaData = metaDataOpt.getOrElse(MetaData(logicalGraph))
    writeGraphHeads(logicalGraph.graphHead, metaData.graphHeadMetaData, saveMode)
    writeVertices(logicalGraph.vertices, metaData.vertexMetaData, saveMode)
    writeEdges(logicalGraph.edges, metaData.edgeMetaData, saveMode)
    CsvMetaDataSink(csvPath).write(metaData, saveMode)
  }

  override def write(graphCollection: L#GC, saveMode: SaveMode): Unit = {
    val metaData = metaDataOpt.getOrElse(MetaData(graphCollection))
    writeGraphHeads(graphCollection.graphHeads, metaData.graphHeadMetaData, saveMode)
    writeVertices(graphCollection.vertices, metaData.vertexMetaData, saveMode)
    writeEdges(graphCollection.edges, metaData.edgeMetaData, saveMode)
    CsvMetaDataSink(csvPath).write(metaData, saveMode)
  }

  def writeGraphHeads(graphHeads: Dataset[L#G], metaData: Dataset[ElementMetaData], saveMode: SaveMode): Unit = {
    val df = graphHeads.join(metaData, ColumnNames.LABEL)

    val strings = df.select(
      idToStr(df(ColumnNames.ID)),
      labelToStr(df(ColumnNames.LABEL)),
      propertiesToStr(df(ColumnNames.PROPERTIES), df(ElementMetaData.metaData)))

    strings
      .write
      .options(options)
      .mode(saveMode)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.GRAPH_HEAD_FILE)
  }

  def writeVertices(vertices: Dataset[L#V], metaData: Dataset[ElementMetaData], saveMode: SaveMode): Unit = {
    val df = vertices.join(metaData, ColumnNames.LABEL)

    val strings = df.select(
      idToStr(df(ColumnNames.ID)),
      graphIdsToStr(df(ColumnNames.GRAPH_IDS)),
      labelToStr(df(ColumnNames.LABEL)),
      propertiesToStr(df(ColumnNames.PROPERTIES), df(ElementMetaData.metaData)))

    strings
      .write
      .options(options)
      .mode(saveMode)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.VERTEX_FILE)
  }

  def writeEdges(edges: Dataset[L#E], metaData: Dataset[ElementMetaData], saveMode: SaveMode): Unit = {
    val df = edges.join(metaData, ColumnNames.LABEL)

    val strings = df.select(
      idToStr(df(ColumnNames.ID)),
      graphIdsToStr(df(ColumnNames.GRAPH_IDS)),
      idToStr(df(ColumnNames.SOURCE_ID)),
      idToStr(df(ColumnNames.TARGET_ID)),
      labelToStr(df(ColumnNames.LABEL)),
      propertiesToStr(df(ColumnNames.PROPERTIES), df(ElementMetaData.metaData)))

    strings
      .write
      .options(options)
      .mode(saveMode)
      .csv(csvPath + CsvConstants.DIRECTORY_SEPARATOR + CsvConstants.EDGE_FILE)
  }
}

object CsvDataSink {

  def apply[L <: Gve[L]](csvPath: String, config: GradoopSparkConfig[L]): CsvDataSink[L] = {
    new CsvDataSink(csvPath, config, None)
  }

  def apply[L <: Gve[L]](csvPath: String, config: GradoopSparkConfig[L], metadata: MetaData): CsvDataSink[L] = {
    new CsvDataSink(csvPath, config, Some(metadata))
  }

  /** Returns CSV string of properties using the corresponding metadata.
   *
   * If the property has a type different type than described in the metadata, an empty string is returned.
   * This is done to prevent parsing errors when a property exists with different types.
   *
   * @param properties properties (Row = PropertyValue)
   * @param metaData metadata for this property (Row = PropertyMetaData)
   * @return string representing the properties
   */
  private def propertiesToString(properties: Map[String, Row], metaData: Seq[Row]): String = {
    metaData.map(m => {
      val prop = properties.get(m(0).asInstanceOf[String])
      if(prop.isEmpty) ""
      else {
        val bytes = prop.get(0).asInstanceOf[Array[Byte]]
        val p = new PropertyValue(bytes)
        if(p.getExactType.string != m(1).asInstanceOf[String]) ""
        else propertyToString(p)
      }
    }).mkString(CsvConstants.VALUE_DELIMITER)
  }

  /** Returns CSV string representation of PropertyValue
   *
   * @param propertyValue property value
   * @return string representation
   */
  private def propertyToString(propertyValue: PropertyValue): String = {
    propertyValue.get match {
      case null => GradoopConstants.NULL_STRING
      case string: String => StringEscaper.escape(string, CsvConstants.ESCAPED_CHARS)
      case list: List[_] => list.asInstanceOf[List[PropertyValue]]
        .map(e => propertyToString(e)).mkString("[", CsvConstants.LIST_DELIMITER, "]")
      case set: Set[_] => set.asInstanceOf[Set[PropertyValue]]
        .map(e => propertyToString(e)).mkString("[", CsvConstants.LIST_DELIMITER, "]")
      case map: Map[_, _] => map.asInstanceOf[Map[PropertyValue, PropertyValue]]
        .map(m => propertyToString(m._1) + CsvConstants.MAP_SEPARATOR.toString + propertyToString(m._2))
        .mkString("{", CsvConstants.LIST_DELIMITER, "}")
      case any: Any => any.toString
    }
  }
}
