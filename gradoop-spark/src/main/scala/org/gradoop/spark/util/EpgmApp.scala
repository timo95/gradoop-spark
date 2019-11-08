package org.gradoop.spark.util

import org.apache.spark.sql.SparkSession
import org.gradoop.common.model.api.types.ComponentTypes
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.{GraphCollectionFactory, LogicalGraphFactory}
import org.gradoop.spark.model.impl.layouts.EpgmGveLayout
import org.gradoop.spark.model.impl.types.EpgmTypes

trait EpgmApp extends EpgmTypes with ComponentTypes {

  private var config: GradoopSparkConfig[G, V, E, LG, GC] = _

  def gveConfig(implicit session: SparkSession): GradoopSparkConfig[G, V, E, LG, GC] = {
    if (config == null) {
      config = new GradoopSparkConfig[G, V, E, LG, GC](null, null)

      config.logicalGraphFactory = new LogicalGraphFactory[G, V, E, LG, GC](EpgmGveLayout, config)
      config.graphCollectionFactory = new GraphCollectionFactory[G, V, E, LG, GC](EpgmGveLayout, config)
    }
    config
  }
}
