package org.gradoop.spark.model.impl.gve

import org.apache.spark.sql.{Dataset, SparkSession}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.GraphCollection
import org.gradoop.spark.model.api.layouts.gve.GveGraphCollectionOperators

class EpgmGveGraphCollectionFactory(var config: GradoopSparkConfig[L])(implicit session: SparkSession)
  extends EpgmGveLayoutFactory[L#GC] {

  override def init(graphHead: Dataset[L#G], vertices: Dataset[L#V], edges: Dataset[L#E]): L#GC = {
    new GraphCollection[L](new EpgmGveLayout(graphHead, vertices, edges), config) with GveGraphCollectionOperators[L]
  }
}
