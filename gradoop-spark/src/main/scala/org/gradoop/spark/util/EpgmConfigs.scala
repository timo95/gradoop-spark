package org.gradoop.spark.util

import org.apache.spark.sql.SparkSession
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.{GraphCollection, GraphCollectionFactory, LogicalGraph, LogicalGraphFactory}
import org.gradoop.spark.model.impl.gve.EpgmGveLayoutFactory
import org.gradoop.spark.model.impl.types.EpgmGveLayoutType

trait EpgmConfigs {
  type L = EpgmGveLayoutType // default layout

  private var _gveConfig: Option[GradoopSparkConfig[L]] = None

  protected def gveConfig(implicit session: SparkSession): GradoopSparkConfig[L] = {
    if (_gveConfig.isEmpty) {
      val lgFac = new EpgmGveLayoutFactory[LogicalGraph[L]](null, new LogicalGraphFactory[L])
      val gcFac = new EpgmGveLayoutFactory[GraphCollection[L]](null, new GraphCollectionFactory[L])
      val config = new GradoopSparkConfig[L](lgFac, gcFac)
      lgFac.config = config
      gcFac.config = config

      _gveConfig = Some(config)
    }
    _gveConfig.get
  }

  //protected def tflConfig(implicit session: SparkSession): GradoopSparkConfig[]
}
