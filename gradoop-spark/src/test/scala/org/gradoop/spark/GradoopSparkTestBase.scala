package org.gradoop.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.gradoop.common.id.GradoopId
import org.gradoop.common.properties.PropertyValue
import org.gradoop.spark.model.impl.gve.{EpgmGveEdge, EpgmGveGraphHead, EpgmGveVertex}
import org.gradoop.spark.model.impl.tfl.{EpgmTflEdge, EpgmTflGraphHead, EpgmTflVertex}
import org.gradoop.spark.model.impl.types.Gve
import org.scalatest.{BeforeAndAfterAll, FunSpec}

trait GradoopSparkTestBase[L <: Gve[L]] extends FunSpec with BeforeAndAfterAll with GradoopSparkTestUtilities[L] {

  protected implicit var sparkSession: SparkSession = _

  override protected def beforeAll: Unit = {
    val conf = new SparkConf()
      .setAppName("Test Application")
      .setMaster("local[4]")

    sparkSession = SparkSession.builder.config(conf)
      .getOrCreate()
  }
}
