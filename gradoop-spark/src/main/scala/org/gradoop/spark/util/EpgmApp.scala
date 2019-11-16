package org.gradoop.spark.util

import org.apache.spark.sql.SparkSession
import org.gradoop.common.model.api.types.ComponentTypes
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.{GraphCollectionFactory, LogicalGraphFactory}
import org.gradoop.spark.model.impl.layouts.EpgmGveLayout
import org.gradoop.spark.model.impl.types.{EpgmGveGraphLayout, OldEpgmModel}

trait EpgmApp extends ComponentTypes {
  type L = EpgmGveGraphLayout

  implicit val session: SparkSession = SparkSession.builder
    .appName("Simple Application")
    .master("local[4]")
    .getOrCreate()

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
