package org.gradoop.spark.io.impl.metadata

import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
import org.gradoop.common.model.api.elements.AttributedElement
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.expressions.filter.FilterExpressions
import org.gradoop.spark.io.impl.csv.CsvConstants
import org.gradoop.spark.model.impl.types.Gve
import org.gradoop.spark.util.StringEscaper

class MetaData(val graphHeadMetaData: Dataset[ElementMetaData],
               val vertexMetaData: Dataset[ElementMetaData],
               val edgeMetaData: Dataset[ElementMetaData]) extends Serializable {

  def getGraphHeadMetaData(label: String): Dataset[ElementMetaData] = {
    graphHeadMetaData.filter(FilterExpressions.hasLabel(label))
  }

  def getVertexMetaData(label: String): Dataset[ElementMetaData] = {
    vertexMetaData.filter(FilterExpressions.hasLabel(label))
  }

  def getEdgeMetaData(label: String): Dataset[ElementMetaData] = {
    edgeMetaData.filter(FilterExpressions.hasLabel(label))
  }
}

object MetaData {

  def apply[L <: Gve[L]](logicalGraph: L#LG): MetaData = {
    import logicalGraph.config.Implicits._
    new MetaData(fromElements(logicalGraph.graphHead),
      fromElements(logicalGraph.vertices),
      fromElements(logicalGraph.edges))
  }

  def apply[L <: Gve[L]](graphCollection: L#GC): MetaData = {
    import graphCollection.config.Implicits._
    new MetaData(fromElements(graphCollection.graphHeads),
      fromElements(graphCollection.vertices),
      fromElements(graphCollection.edges))
  }

  private def fromElements[EL <: AttributedElement](dataset: Dataset[EL])
    (implicit session: SparkSession): Dataset[ElementMetaData] = {
    import ColumnNames._
    import org.apache.spark.sql.functions._
    import org.gradoop.spark.util.Implicits._
    import session.implicits._

    // Row = PropertyValue
    val getTypeString = udf((r: Row) =>
      if(r == null) null else new PropertyValue(r(0).asInstanceOf[Array[Byte]]).getExactType.string)

    // If all elements are null, it returns null. Otherwise a struct.
    def structOrNull(cols: Column*): Column = when(cols.map(c => isnull(c)).reduce(_&&_), lit(null))
      .otherwise(struct(cols: _*))

    dataset
      // one row for each property per element
      .select(dataset.label, explode_outer(dataset.properties).as(Seq(PropertyMetaData.key, "property")))
      // put property key and type in struct
      .select(col(LABEL), structOrNull(col(PropertyMetaData.key), getTypeString(col(s"property"))
        .as(PropertyMetaData.typeString)).as("property"))
      // group by label
      .groupBy(LABEL)
      // aggregate property structs to a set per label
      .agg(collect_set("property").as(ElementMetaData.metaData))
      .as[ElementMetaData]
  }
}
