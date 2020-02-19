package org.gradoop.spark.util

import org.apache.spark.sql.SparkSession
import org.gradoop.common.model.api.components.ComponentTypes
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.gve.{EpgmGveGraphCollectionFactory, EpgmGveLogicalGraphFactory}
import org.gradoop.spark.model.impl.tfl.{EpgmTflGraphCollectionFactory, EpgmTflLogicalGraphFactory}
import org.gradoop.spark.model.impl.types.{EpgmGve, EpgmTfl}

trait EpgmConfigs extends ComponentTypes {
  type LGve = EpgmGve
  type LTfl = EpgmTfl

  private var _gveConfig: Option[GradoopSparkConfig[LGve]] = None

  private var _tflConfig: Option[GradoopSparkConfig[LTfl]] = None

  protected def gveConfig(implicit session: SparkSession): GradoopSparkConfig[LGve] = {
    if (_gveConfig.isEmpty) {
      val lgFac = new EpgmGveLogicalGraphFactory(null)
      val gcFac = new EpgmGveGraphCollectionFactory(null)
      val config = new GradoopSparkConfig[LGve](lgFac, gcFac)
      lgFac.config = config
      gcFac.config = config

      _gveConfig = Some(config)
    }
    _gveConfig.get
  }

  protected def tflConfig(implicit session: SparkSession): GradoopSparkConfig[LTfl] = {
    if (_tflConfig.isEmpty) {
      val lgFac = new EpgmTflLogicalGraphFactory(null)
      val gcFac = new EpgmTflGraphCollectionFactory(null)
      val config = new GradoopSparkConfig[LTfl](lgFac, gcFac)
      lgFac.config = config
      gcFac.config = config

      _tflConfig = Some(config)
    }
    _tflConfig.get
  }
}
