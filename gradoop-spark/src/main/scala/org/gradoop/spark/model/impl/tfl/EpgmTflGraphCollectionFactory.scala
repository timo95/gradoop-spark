package org.gradoop.spark.model.impl.tfl

import org.apache.spark.sql.{Dataset, SparkSession}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.GraphCollection
import org.gradoop.spark.model.api.layouts.tfl.TflGraphCollectionOperators

class EpgmTflGraphCollectionFactory(var config: GradoopSparkConfig[L])(implicit session: SparkSession)
  extends EpgmTflLayoutFactory[L#GC] {

  override def init(graphHeads: Map[String, Dataset[L#G]],
    vertices: Map[String, Dataset[L#V]],
    edges: Map[String, Dataset[L#E]],
    graphHeadProperties: Map[String, Dataset[L#P]],
    vertexProperties: Map[String, Dataset[L#P]],
    edgeProperties: Map[String, Dataset[L#P]]): L#GC = {
    new GraphCollection[L](new EpgmTflLayout(graphHeads, vertices, edges, graphHeadProperties, vertexProperties,
      edgeProperties), config) with TflGraphCollectionOperators[L]
  }
}
