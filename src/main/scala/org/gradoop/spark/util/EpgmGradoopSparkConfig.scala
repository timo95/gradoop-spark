package org.gradoop.spark.util

import org.apache.spark.sql.SparkSession
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.{GraphCollectionFactory, LogicalGraphFactory}
import org.gradoop.spark.model.impl.layouts.EpgmGveLayoutFactory

class EpgmGradoopSparkConfig()(implicit session: SparkSession) extends GradoopSparkConfig[G, V, E, LG, GC] {

  override var logicalGraphFactory: LogicalGraphFactory[G, V, E, LG, GC] = new LogicalGraphFactory[G, V, E, LG, GC](new EpgmGveLayoutFactory)
  override var graphCollectionFactory: GraphCollectionFactory[G, V, E, LG, GC] = new GraphCollectionFactory[G, V, E, LG, GC](new EpgmGveLayoutFactory)

}
