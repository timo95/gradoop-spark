package org.gradoop.spark.model.impl.gve

import org.apache.spark.sql.{Dataset, SparkSession}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.LogicalGraph
import org.gradoop.spark.model.api.layouts.gve.GveLogicalGraphOperators

class EpgmGveLogicalGraphFactory(var config: GradoopSparkConfig[L])(implicit session: SparkSession)
  extends EpgmGveLayoutFactory[L#LG] {

  override def init(graphHeads: Dataset[L#G], vertices: Dataset[L#V], edges: Dataset[L#E]): L#LG = {
    new LogicalGraph[L](new EpgmGveLayout(graphHeads, vertices, edges), config) with GveLogicalGraphOperators[L]
  }
}
