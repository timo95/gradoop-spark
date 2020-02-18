package org.gradoop.spark.io.impl.metadata

import org.apache.spark.sql.{Column, Dataset, Row, SparkSession}
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
        fromElements(gve.edges))
      case tfl: TflLayout[L] => new MetaData(
        TflFunctions.reduceUnion(tfl.graphHeadProperties.values.map(p => fromElements(p))),
        TflFunctions.reduceUnion(tfl.vertexProperties.values.map(p => fromElements(p))),
        TflFunctions.reduceUnion(tfl.edgeProperties.values.map(p => fromElements(p))))
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

    dataset
      // one row for each property per element
      .select(dataset.label, explode_outer(dataset.properties).as(Seq(PropertyMetaData.key, "property")))
      // put property key and type in struct
      .select(col(LABEL), structOrNull(col(PropertyMetaData.key), getTypeString(col(s"property"))
        .as(PropertyMetaData.typeString)).as("propertyType"))
      // group by label
      .groupBy(LABEL)
      // aggregate property structs to a set per label
      .agg(collect_set("propertyType").as(ElementMetaData.metaData))
      .as[ElementMetaData]
  }
}
