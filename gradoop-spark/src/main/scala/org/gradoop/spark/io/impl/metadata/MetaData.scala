package org.gradoop.spark.io.impl.metadata

import org.apache.spark.sql.{Dataset, SparkSession}
import org.gradoop.common.model.api.gve.GveElement
import org.gradoop.common.properties.Type
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.expressions.filter.FilterStrings
import org.gradoop.spark.model.impl.types.Gve

class MetaData(val graphHeadMetaData: Dataset[ElementMetaData],
               val vertexMetaData: Dataset[ElementMetaData],
               val edgeMetaData: Dataset[ElementMetaData]) extends Serializable {

  def getGraphHeadMetaData(label: String): Dataset[ElementMetaData] = {
    graphHeadMetaData.filter(FilterStrings.hasLabel(label))
  }

  def getVertexMetaData(label: String): Dataset[ElementMetaData] = {
    vertexMetaData.filter(FilterStrings.hasLabel(label))
  }

  def getEdgeMetaData(label: String): Dataset[ElementMetaData] = {
    edgeMetaData.filter(FilterStrings.hasLabel(label))
  }
}

object MetaData {

  def apply[L <: Gve[L]](logicalGraph: L#LG): MetaData = {
    import logicalGraph.config.implicits._
    new MetaData(fromElements(logicalGraph.graphHead),
      fromElements(logicalGraph.vertices),
      fromElements(logicalGraph.edges))
  }

  def apply[L <: Gve[L]](graphCollection: L#GC): MetaData = {
    import graphCollection.config.implicits._
    new MetaData(fromElements(graphCollection.graphHeads),
      fromElements(graphCollection.vertices),
      fromElements(graphCollection.edges))
  }

  private def fromElements[EL <: GveElement]
  (dataset: Dataset[EL])(implicit session: SparkSession): Dataset[ElementMetaData] = {
    import ColumnNames._
    import org.apache.spark.sql.functions._
    import org.gradoop.spark.util.Implicits._
    import session.implicits._

    val byteToString = udf((b: Byte) => Type(b).string)

    dataset
      // one row for each property per element
      .select(dataset.label, explode(dataset.properties).as(Seq("key","property")))
      // put property key and type in struct
      .select(col(LABEL), struct(col("key"), byteToString(col(s"property.$PROPERTY_TYPE")).as("typeString")).as("property"))
      // group by label
      .groupBy(LABEL)
      // aggregate property structs to a set per label
      .agg(collect_set("property").as("properties"))
      .as[ElementMetaData]
    // TODO reduce dependency on col names being equal to case class parameter names - reflection?
  }
}
