package org.gradoop.spark.io.impl.metadata

import org.apache.spark.sql.{Dataset, SparkSession}
import org.gradoop.common.model.api.elements.{Edge, Element, GraphHead, Vertex}
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}

class MetaData(graphHeadMetaData: Dataset[ElementMetaData],
               vertexMetaData: Dataset[ElementMetaData],
               edgeMetaData: Dataset[ElementMetaData]) {
}

object MetaData {

  def apply[
    G <: GraphHead,
    V <: Vertex,
    E <: Edge,
    LG <: LogicalGraph[G, V, E, LG, GC],
    GC <: GraphCollection[G, V, E, LG, GC]](logicalGraph: LG): MetaData = {
    import logicalGraph.config.implicits._
    new MetaData(fromElements(logicalGraph.graphHead),
      fromElements(logicalGraph.vertices),
      fromElements(logicalGraph.edges))
  }

  def apply[
    G <: GraphHead,
    V <: Vertex,
    E <: Edge,
    LG <: LogicalGraph[G, V, E, LG, GC],
    GC <: GraphCollection[G, V, E, LG, GC]](graphCollection: GC): MetaData = {
    import graphCollection.config.implicits._
    new MetaData(fromElements(graphCollection.graphHeads),
      fromElements(graphCollection.vertices),
      fromElements(graphCollection.edges))
  }

  private def fromElements[EL <: Element]
  (dataset: Dataset[EL])(implicit session: SparkSession): Dataset[ElementMetaData] = {
    import org.apache.spark.sql.functions._
    import org.gradoop.spark.util.Implicits._
    import session.implicits._
    import ColumnNames._

    dataset
      // one row for each property per element
      .select(dataset.labels, explode(dataset.properties).as(Seq("key","property")))
      // put property key and type in struct
      .select(col(LABEL), struct("key", s"property.$PROPERTY_TYPE").as("property"))
      // group by label
      .groupBy(LABEL)
      // aggregate property structs to a set per label
      .agg(collect_set("property").as("properties"))
      .as[ElementMetaData]
    // TODO reduce dependency on col names being equal to case class parameter names - reflection?
  }
}
