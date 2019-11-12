package org.gradoop.spark.benchmarks

import org.apache.spark.sql.SparkSession
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.{GraphCollectionFactory, LogicalGraphFactory}
import org.gradoop.spark.model.impl.layouts.EpgmGveLayout
import org.gradoop.spark.model.impl.types.EpgmTypes

trait BaseBenchmark extends EpgmTypes {

  private var _gveConfig: GradoopSparkConfig[G, V, E, LG, GC] = _

  def gveConfig(implicit session: SparkSession): GradoopSparkConfig[G, V, E, LG, GC] = {
    if (_gveConfig == null) {
      _gveConfig = new GradoopSparkConfig[G, V, E, LG, GC](null, null)

      _gveConfig.logicalGraphFactory = new LogicalGraphFactory[G, V, E, LG, GC](EpgmGveLayout, _gveConfig)
      _gveConfig.graphCollectionFactory = new GraphCollectionFactory[G, V, E, LG, GC](EpgmGveLayout, _gveConfig)
    }
    _gveConfig
  }
}
