package org.gradoop.spark.model.api.config

import org.apache.spark.sql.{Column, Dataset, Encoder, SparkSession}
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Identifiable, Vertex}
import org.gradoop.common.model.api.types.ComponentTypes
import org.gradoop.spark.model.api.graph.{GraphCollection, GraphCollectionFactory, LogicalGraph, LogicalGraphFactory}
import org.gradoop.spark.model.api.layouts.{GraphCollectionLayoutFactory, LogicalGraphLayoutFactory}
import org.gradoop.spark.util.{ColumnSelector, DisplayConverter}

class GradoopSparkConfig[
  G <: GraphHead,
  V <: Vertex,
  E <: Edge,
  LG <: LogicalGraph[G, V, E, LG, GC],
  GC <: GraphCollection[G, V, E, LG, GC]]
(var logicalGraphFactory: LogicalGraphFactory[G, V, E, LG, GC],
 var graphCollectionFactory: GraphCollectionFactory[G, V, E, LG, GC])
(implicit val sparkSession: SparkSession) extends Serializable {

  object implicits extends Serializable with ComponentTypes {
    // Spark session
    implicit def implicitSparkSession: SparkSession = sparkSession

    // Encoder
    implicit def implicitGraphHeadEncoder: Encoder[G] = graphHeadEncoder
    implicit def impliticVertexEncoder: Encoder[V] = vertexEncoder
    implicit def implicitEdgeEncoder: Encoder[E] = edgeEncoder

    // Wrappers
    implicit def columnSelector[T](dataset: Dataset[T]): ColumnSelector[T] = new ColumnSelector[T](dataset)
    implicit def displayConverter[T <: Identifiable](dataset: Dataset[T]): DisplayConverter[T] = new DisplayConverter[T](dataset)
  }

  def graphHeadEncoder: Encoder[G] = logicalGraphFactory.graphHeadEncoder

  def vertexEncoder: Encoder[V] = logicalGraphFactory.vertexEncoder

  def edgeEncoder: Encoder[E] = logicalGraphFactory.edgeEncoder
}

object GradoopSparkConfig {

  def create[
    G <: GraphHead,
    V <: Vertex,
    E <: Edge,
    LG <: LogicalGraph[G, V, E, LG, GC],
    GC <: GraphCollection[G, V, E, LG, GC]]
  (logicalGraphLayoutFactory: LogicalGraphLayoutFactory[G, V, E, LG, GC],
   graphCollectionLayoutFactory: GraphCollectionLayoutFactory[G, V, E, LG, GC])
  (implicit sparkSession: SparkSession): GradoopSparkConfig[G, V, E, LG, GC] = {
    val config = new GradoopSparkConfig[G, V, E, LG, GC](null, null)
    config.logicalGraphFactory = new LogicalGraphFactory[G, V, E, LG, GC](logicalGraphLayoutFactory, config)
    config.graphCollectionFactory = new GraphCollectionFactory[G, V, E, LG, GC](graphCollectionLayoutFactory, config)
    config
  }
}