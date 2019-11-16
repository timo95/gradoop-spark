package org.gradoop.spark.benchmarks

import org.apache.spark.sql.SparkSession
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.{GraphCollectionFactory, LogicalGraphFactory}
import org.gradoop.spark.model.impl.layouts.EpgmGveLayout
import org.gradoop.spark.model.impl.types.EpgmGveGraphLayout

trait BaseBenchmark {
  type L = EpgmGveGraphLayout

  private var _gveConfig: GradoopSparkConfig[L] = _

  def gveConfig(implicit session: SparkSession): GradoopSparkConfig[L] = {
    if (_gveConfig == null) {
      _gveConfig = new GradoopSparkConfig[L](null, null)

      _gveConfig.logicalGraphFactory = new LogicalGraphFactory[L](EpgmGveLayout, _gveConfig)
      _gveConfig.graphCollectionFactory = new GraphCollectionFactory[L](EpgmGveLayout, _gveConfig)
    }
    _gveConfig
  }
}
