package org.gradoop.spark.model.api.config

import org.apache.spark.sql.{Encoder, SparkSession}
import org.gradoop.common.model.api.ComponentTypes
import org.gradoop.spark.model.api.graph.{GraphCollectionFactory, LogicalGraphFactory}
import org.gradoop.spark.model.api.layouts.{GraphCollectionLayoutFactory, GveBaseLayoutFactory, LogicalGraphLayoutFactory}
import org.gradoop.spark.model.impl.types.GveLayoutType
import org.gradoop.spark.util.Implicits

class GradoopSparkConfig[L <: GveLayoutType]
(var logicalGraphFactory: LogicalGraphFactory[L], var graphCollectionFactory: GraphCollectionFactory[L])
(implicit val sparkSession: SparkSession) extends Serializable {

  object implicits extends Implicits with ComponentTypes {
    // Spark session
    implicit def implicitSparkSession: SparkSession = sparkSession

    // Encoder
    implicit def implicitGraphHeadEncoder: Encoder[L#G] = graphHeadEncoder
    implicit def impliticVertexEncoder: Encoder[L#V] = vertexEncoder
    implicit def implicitEdgeEncoder: Encoder[L#E] = edgeEncoder
  }

  def graphHeadEncoder: Encoder[L#G] = logicalGraphFactory.layoutFactory.graphHeadEncoder

  def vertexEncoder: Encoder[L#V] = logicalGraphFactory.layoutFactory.vertexEncoder

  def edgeEncoder: Encoder[L#E] = logicalGraphFactory.layoutFactory.edgeEncoder
}

object GradoopSparkConfig {

  def create[L <: GveLayoutType](logicalGraphLayoutFactory: GveBaseLayoutFactory[L] with LogicalGraphLayoutFactory[L],
                                 graphCollectionLayoutFactory: GveBaseLayoutFactory[L] with GraphCollectionLayoutFactory[L])
                                (implicit sparkSession: SparkSession): GradoopSparkConfig[L] = {
    val config = new GradoopSparkConfig[L](null, null)
    config.logicalGraphFactory = new LogicalGraphFactory[L](logicalGraphLayoutFactory, config)
    config.graphCollectionFactory = new GraphCollectionFactory[L](graphCollectionLayoutFactory, config)
    config
  }
}