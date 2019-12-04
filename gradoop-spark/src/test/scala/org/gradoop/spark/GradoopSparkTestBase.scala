package org.gradoop.spark

import org.apache.spark.sql.SparkSession
import org.gradoop.common.GradoopTestUtils
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.types.Gve
import org.gradoop.spark.util.SparkAsciiGraphLoader
import org.scalatest.FunSpec


trait GradoopSparkTestBase[L <: Gve[L]] extends FunSpec {

  protected implicit val session: SparkSession = SparkSession.builder
    .appName("Simple Application")
    .master("local[4]")
    .getOrCreate()

  protected def getConfig: GradoopSparkConfig[L]

  protected def getSocialNetworkLoader: SparkAsciiGraphLoader[L] = {
    SparkAsciiGraphLoader.fromStream(getConfig, getClass.getResourceAsStream(GradoopTestUtils.SOCIAL_NETWORK_GDL_FILE))
  }
}
