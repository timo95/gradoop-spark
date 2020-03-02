package org.gradoop.spark.io.impl.csv

import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, Row, SaveMode}
import org.gradoop.common.id.GradoopId
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.{ColumnNames, GradoopConstants}
import org.gradoop.spark.io.impl.csv.CsvConstants._
import org.gradoop.spark.util.StringEscaper

class CsvDataSinkBase(csvPath: String) {

  private val options: Map[String, String] = Map(
    "sep" -> TOKEN_DELIMITER,
    "nullValue" -> GradoopConstants.NULL_STRING,
    "escapeQuotes" -> "false",
    "emptyValue" -> "")

  private val idToStr = udf((id: Row) => new GradoopId(id(0).asInstanceOf[Array[Byte]]).toString)
  private val graphIdsToStr = udf((graphIds: Seq[Row]) => graphIds
    .map(id => new GradoopId(id(0).asInstanceOf[Array[Byte]]).toString)
    .mkString("[", LIST_DELIMITER, "]"))
  private val labelToStr = udf(label => StringEscaper.escape(label, ESCAPED_CHARS))
  protected val propertiesToStrUdf = udf((properties: Map[String, Row], metaData: Seq[Row]) =>
    CsvDataSinkBase.propertiesToString(properties, metaData))

  protected def writeGraphHeads(localPath: String, graphHeads: DataFrame, saveMode: SaveMode): Unit = {
    val strings = graphHeads.select(
      idToStr(col(ColumnNames.ID)),
      labelToStr(col(ColumnNames.LABEL)),
      col(ColumnNames.PROPERTIES))

    strings
      .write
      .options(options)
      .mode(saveMode)
      .csv(csvPath + DIRECTORY_SEPARATOR + localPath)
  }

  protected def writeVertices(localPath: String, vertices: DataFrame, saveMode: SaveMode): Unit = {
    val strings = vertices.select(
      idToStr(col(ColumnNames.ID)),
      graphIdsToStr(col(ColumnNames.GRAPH_IDS)),
      labelToStr(col(ColumnNames.LABEL)),
      col(ColumnNames.PROPERTIES))

    strings
      .write
      .options(options)
      .mode(saveMode)
      .csv(csvPath + DIRECTORY_SEPARATOR + localPath)
  }

  protected def writeEdges(localPath: String, edges: DataFrame, saveMode: SaveMode): Unit = {
    val strings = edges.select(
      idToStr(col(ColumnNames.ID)),
      graphIdsToStr(col(ColumnNames.GRAPH_IDS)),
      idToStr(col(ColumnNames.SOURCE_ID)),
      idToStr(col(ColumnNames.TARGET_ID)),
      labelToStr(col(ColumnNames.LABEL)),
      col(ColumnNames.PROPERTIES))

    strings
      .write
      .options(options)
      .mode(saveMode)
      .csv(csvPath + DIRECTORY_SEPARATOR + localPath)
  }
}

object CsvDataSinkBase {

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
    }).mkString(VALUE_DELIMITER)
  }

  /** Returns CSV string representation of PropertyValue
   *
   * @param propertyValue property value
   * @return string representation
   */
  private def propertyToString(propertyValue: PropertyValue): String = {
    propertyValue.get match {
      case null => GradoopConstants.NULL_STRING
      case string: String => StringEscaper.escape(string, ESCAPED_CHARS)
      case list: List[_] => list.asInstanceOf[List[PropertyValue]]
        .map(e => propertyToString(e)).mkString("[", LIST_DELIMITER, "]")
      case set: Set[_] => set.asInstanceOf[Set[PropertyValue]]
        .map(e => propertyToString(e)).mkString("[", LIST_DELIMITER, "]")
      case map: Map[_, _] => map.asInstanceOf[Map[PropertyValue, PropertyValue]]
        .map(m => propertyToString(m._1) + MAP_SEPARATOR.toString + propertyToString(m._2))
        .mkString("{", LIST_DELIMITER, "}")
      case any: Any => any.toString
    }
  }
}
