package org.gradoop.spark.model.api.config

import org.apache.spark.sql.{Encoder, SparkSession}
import org.gradoop.common.model.api.ComponentTypes
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}
import org.gradoop.spark.model.api.layouts.{GraphCollectionLayoutFactory, GveBaseLayoutFactory, LogicalGraphLayoutFactory}
import org.gradoop.spark.model.impl.types.GveLayoutType
import org.gradoop.spark.util.Implicits

class GradoopSparkConfig[L <: GveLayoutType]
(var logicalGraphLayoutFactory: GveBaseLayoutFactory[L, LogicalGraph[L]] with LogicalGraphLayoutFactory[L],
 var graphCollectionLayoutFactory: GveBaseLayoutFactory[L, GraphCollection[L]] with GraphCollectionLayoutFactory[L])
(implicit val sparkSession: SparkSession) extends Serializable {

  object implicits extends Implicits with ComponentTypes {
    // Spark session
    implicit def implicitSparkSession: SparkSession = sparkSession

    // Encoder
    implicit def implicitGraphHeadEncoder: Encoder[L#G] = graphHeadEncoder
    implicit def impliticVertexEncoder: Encoder[L#V] = vertexEncoder
    implicit def implicitEdgeEncoder: Encoder[L#E] = edgeEncoder
  }

  def graphHeadEncoder: Encoder[L#G] = logicalGraphLayoutFactory.graphHeadEncoder

  def vertexEncoder: Encoder[L#V] = logicalGraphLayoutFactory.vertexEncoder

  def edgeEncoder: Encoder[L#E] = logicalGraphLayoutFactory.edgeEncoder

  def logicalGraphFactory: GveBaseLayoutFactory[L, LogicalGraph[L]] with LogicalGraphLayoutFactory[L] = logicalGraphLayoutFactory
  def graphCollectionFactory: GveBaseLayoutFactory[L, GraphCollection[L]] with GraphCollectionLayoutFactory[L] = graphCollectionLayoutFactory
}

object GradoopSparkConfig {

}