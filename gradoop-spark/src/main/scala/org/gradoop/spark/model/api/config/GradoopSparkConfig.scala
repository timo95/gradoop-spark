package org.gradoop.spark.model.api.config

import org.apache.spark.sql.SparkSession
import org.gradoop.common.model.api.ComponentTypes
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}
import org.gradoop.spark.model.api.layouts.{GraphCollectionLayoutFactory, GveBaseLayoutFactory, LogicalGraphLayoutFactory}
import org.gradoop.spark.model.impl.types.GveLayoutType
import org.gradoop.spark.util.Implicits

class GradoopSparkConfig[L <: GveLayoutType]
(val logicalGraphFactory: GveBaseLayoutFactory[L, LogicalGraph[L]] with LogicalGraphLayoutFactory[L],
 val graphCollectionFactory: GveBaseLayoutFactory[L, GraphCollection[L]] with GraphCollectionLayoutFactory[L])
(implicit val sparkSession: SparkSession) extends Serializable {

  object implicits extends Implicits with ComponentTypes {
    // Spark session
    implicit def implicitSparkSession: SparkSession = sparkSession
  }
}
