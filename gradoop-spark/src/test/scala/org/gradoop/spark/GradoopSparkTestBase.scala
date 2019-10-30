package org.gradoop.spark

import org.apache.spark.sql.SparkSession
import org.gradoop.common.GradoopTestUtils
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.types.GraphModel
import org.gradoop.spark.util.SparkAsciiGraphLoader
import org.scalatest.FlatSpec


abstract class GradoopSparkTestBase extends FlatSpec {
  this: GraphModel =>

  implicit val session: SparkSession

  def getConfig: GradoopSparkConfig[G, V, E, LG, GC]

  def getSocialNetworkLoader: SparkAsciiGraphLoader[G, V, E, LG, GC] = {
    SparkAsciiGraphLoader.fromFile(getConfig, GradoopTestUtils.SOCIAL_NETWORK_GDL_FILE)
  }
}
