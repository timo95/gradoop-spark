package org.gradoop.spark

import org.apache.spark.sql.SparkSession
import org.gradoop.common.GradoopTestUtils
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.types.GraphModel
import org.gradoop.spark.util.SparkAsciiGraphLoader
import org.scalatest.FunSpec


trait GradoopSparkTestBase extends FunSpec {
  this: GraphModel =>

  protected implicit val session: SparkSession = SparkSession.builder
    .appName("Simple Application")
    .master("local[4]")
    .getOrCreate()

  protected def getConfig: GradoopSparkConfig[G, V, E, LG, GC]

  protected def getSocialNetworkLoader: SparkAsciiGraphLoader[G, V, E, LG, GC] = {
    SparkAsciiGraphLoader.fromStream(getConfig, getClass.getResourceAsStream(GradoopTestUtils.SOCIAL_NETWORK_GDL_FILE))
  }
}
