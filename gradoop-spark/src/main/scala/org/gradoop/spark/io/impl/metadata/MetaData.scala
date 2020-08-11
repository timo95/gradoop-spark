package org.gradoop.spark.io.impl.metadata

import org.apache.spark.sql._
import org.apache.spark.sql.functions.col
import org.gradoop.common.model.api.elements.AttributedElement
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.expressions.FilterExpressions
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}
import org.gradoop.spark.model.api.layouts.gve.GveLayout
import org.gradoop.spark.model.api.layouts.tfl.TflLayout
import org.gradoop.spark.model.impl.types.LayoutType
import org.gradoop.spark.util.TflFunctions

class MetaData(val graphHeadMetaData: Dataset[ElementMetaData],
               val vertexMetaData: Dataset[ElementMetaData],
               val edgeMetaData: Dataset[ElementMetaData]) extends Serializable {

  def graphHeadMetaData(label: String): Dataset[ElementMetaData] = {
    graphHeadMetaData.filter(FilterExpressions.hasLabel(label))
  }

  def vertexMetaData(label: String): Dataset[ElementMetaData] = {
    vertexMetaData.filter(FilterExpressions.hasLabel(label))
  }

  def edgeMetaData(label: String): Dataset[ElementMetaData] = {
    edgeMetaData.filter(FilterExpressions.hasLabel(label))
  }

  def graphHeadLabels(implicit encoder: Encoder[String]): Dataset[String] = {
    graphHeadMetaData.select(col(ElementMetaData.label).as[String])
  }

  def vertexLabels(implicit encoder: Encoder[String]): Dataset[String] = {
    vertexMetaData.select(col(ElementMetaData.label).as[String])
  }

  def edgeLabels(implicit encoder: Encoder[String]): Dataset[String] = {
    edgeMetaData.select(col(ElementMetaData.label).as[String])
  }

  def cache(): MetaData = {
    new MetaData(graphHeadMetaData.cache(), vertexMetaData.cache(), edgeMetaData.cache())
  }
}

object MetaData {

  def apply[L <: LayoutType[L]](logicalGraph: LogicalGraph[L]): MetaData = {
    import logicalGraph.config.Implicits._
    fromLayout[L](logicalGraph.layout)
  }

  def apply[L <: LayoutType[L]](graphCollection: GraphCollection[L]): MetaData = {
    import graphCollection.config.Implicits._
    fromLayout[L](graphCollection.layout)
  }

  def fromLayout[L <: LayoutType[L]](layout: L#L)(implicit sparkSession: SparkSession): MetaData = {
    import sparkSession.implicits._
    layout match {
      case gve: GveLayout[L] => new MetaData(fromElements(gve.graphHead),
        fromElements(gve.vertices),
        fromElements(gve.edges)).cache()
      case tfl: TflLayout[L] => new MetaData(
        TflFunctions.reduceUnion(tfl.graphHeadProperties.values.map(p => fromElements(p))),
        TflFunctions.reduceUnion(tfl.vertexProperties.values.map(p => fromElements(p))),
        TflFunctions.reduceUnion(tfl.edgeProperties.values.map(p => fromElements(p)))).cache()
      case other: Any => throw new IllegalArgumentException("Layout %s is not supported by MetaData."
        .format(other.getClass.getSimpleName))
    }
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

    val PROPERTY = "property"
    val PROPERTY_TYPE = "propertyType"

    dataset
      // one row for each property per element
      .select(dataset.label, explode_outer(dataset.properties).as(Seq(PropertyMetaData.key, PROPERTY)))
      // put property key and type in struct
      .select(col(LABEL), structOrNull(col(PropertyMetaData.key), getTypeString(col(PROPERTY))
        .as(PropertyMetaData.typeString)).as(PROPERTY_TYPE))
      // metadata is described per label
      .groupBy(LABEL)
      // aggregate property structs to a set per label
      .agg(collect_set(PROPERTY_TYPE).as(ElementMetaData.metaData))
      .as[ElementMetaData]
  }
}
