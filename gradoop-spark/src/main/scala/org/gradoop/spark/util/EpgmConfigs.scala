package org.gradoop.spark.util

import org.apache.spark.sql.SparkSession
import org.gradoop.common.model.api.components.ComponentTypes
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.gve.{EpgmGveGraphCollectionFactory, EpgmGveLogicalGraphFactory}
import org.gradoop.spark.model.impl.tfl.{EpgmTflGraphCollectionFactory, EpgmTflLogicalGraphFactory}
import org.gradoop.spark.model.impl.types.{EpgmGve, EpgmTfl}

trait EpgmConfigs extends ComponentTypes {
  type L = EpgmGve // default layout

  private var _gveConfig: Option[GradoopSparkConfig[EpgmGve]] = None

  private var _tflConfig: Option[GradoopSparkConfig[EpgmTfl]] = None

  protected def gveConfig(implicit session: SparkSession): GradoopSparkConfig[EpgmGve] = {
    if (_gveConfig.isEmpty) {
      val lgFac = new EpgmGveLogicalGraphFactory(null)
      val gcFac = new EpgmGveGraphCollectionFactory(null)
      val config = new GradoopSparkConfig[EpgmGve](lgFac, gcFac)
      lgFac.config = config
      gcFac.config = config

      _gveConfig = Some(config)
    }
    _gveConfig.get
  }

  protected def tflConfig(implicit session: SparkSession): GradoopSparkConfig[EpgmTfl] = {
    if (_tflConfig.isEmpty) {
      val lgFac = new EpgmTflLogicalGraphFactory(null)
      val gcFac = new EpgmTflGraphCollectionFactory(null)
      val config = new GradoopSparkConfig[EpgmTfl](lgFac, gcFac)
      lgFac.config = config
      gcFac.config = config

      _tflConfig = Some(config)
    }
    _tflConfig.get
  }
}
