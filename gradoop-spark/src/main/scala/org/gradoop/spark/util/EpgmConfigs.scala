package org.gradoop.spark.util

import org.apache.spark.sql.SparkSession
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.gve.{EpgmGveGraphCollectionFactory, EpgmGveLogicalGraphFactory}
import org.gradoop.spark.model.impl.types.EpgmGve

trait EpgmConfigs {
  type L = EpgmGve // default layout

  private var _gveConfig: Option[GradoopSparkConfig[L]] = None

  protected def gveConfig(implicit session: SparkSession): GradoopSparkConfig[L] = {
    if (_gveConfig.isEmpty) {
      val lgFac = new EpgmGveLogicalGraphFactory(null)
      val gcFac = new EpgmGveGraphCollectionFactory(null)
      val config = new GradoopSparkConfig[L](lgFac, gcFac)
      lgFac.config = config
      gcFac.config = config

      _gveConfig = Some(config)
    }
    _gveConfig.get
  }

  //protected def tflConfig(implicit session: SparkSession): GradoopSparkConfig[]
}
