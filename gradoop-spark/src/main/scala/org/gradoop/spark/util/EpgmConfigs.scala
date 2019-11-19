package org.gradoop.spark.util

import org.apache.spark.sql.SparkSession
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.{GraphCollection, GraphCollectionFactory, LogicalGraph, LogicalGraphFactory}
import org.gradoop.spark.model.impl.gve.EpgmGveLayoutFactory
import org.gradoop.spark.model.impl.types.EpgmGveLayoutType

trait EpgmConfigs {
  type L = EpgmGveLayoutType

  private var _gveConfig: Option[GradoopSparkConfig[L]] = None

  protected def gveConfig(implicit session: SparkSession): GradoopSparkConfig[L] = {
    if (_gveConfig.isEmpty) {
      val config = new GradoopSparkConfig[L](null, null)
      config.logicalGraphLayoutFactory = new EpgmGveLayoutFactory[LogicalGraph[L]](config, new LogicalGraphFactory[L])
      config.graphCollectionLayoutFactory = new EpgmGveLayoutFactory[GraphCollection[L]](config, new GraphCollectionFactory[L])
      _gveConfig = Some(config)
    }
    _gveConfig.get
  }
}
