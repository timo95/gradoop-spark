package org.gradoop.spark

import org.apache.spark.sql.SparkSession
import org.gradoop.spark.model.impl.types.Gve
import org.scalatest.{BeforeAndAfterAll, FunSpec}

trait GradoopSparkTestBase[L <: Gve[L]] extends FunSpec with BeforeAndAfterAll with GradoopSparkTestUtilities[L] {

  protected implicit var sparkSession: SparkSession = _

  override protected def beforeAll: Unit = {
    sparkSession = SparkSession.builder
      .appName("Test Application")
      .master("local[4]")
      .getOrCreate()
  }
}
