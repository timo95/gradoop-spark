package org.gradoop.spark.model.impl.operators.tostring

import org.gradoop.spark.EpgmGradoopSparkTestBase
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.operators.tostring.gve.CanonicalAdjacencyMatrixBuilder
import org.gradoop.spark.model.impl.operators.tostring.gve.ElementToString._
import org.gradoop.spark.util.SparkAsciiGraphLoader

import scala.io.Source

class CanonicalAdjacencyMatrixBuilderTest extends EpgmGradoopSparkTestBase {
  val config: GradoopSparkConfig[L] = getConfig

  describe("Directed") {
    val loader = SparkAsciiGraphLoader
      .fromFile(getConfig, getClass.getResource("/data/gdl/cam_test.gdl").getFile)

    val collection = loader.getGraphCollection

    val cam = new CanonicalAdjacencyMatrixBuilder[L](graphHeadToDataString, vertexToDataString, edgeToDataString,true)

    val result = collection.callForValue(cam)

    val stringPath = "/data/expected/cam_test_directed"
    val expected = Source.fromFile(stringPath)

    assert(expected.mkString eq result)

    expected.close
  }

  describe("Undirected") {
    val loader = SparkAsciiGraphLoader
      .fromFile(getConfig, getClass.getResource("/data/gdl/cam_test.gdl").getFile)

    val collection = loader.getGraphCollection

    val cam = new CanonicalAdjacencyMatrixBuilder[L](graphHeadToDataString, vertexToDataString, edgeToDataString,false)

    val result = collection.callForValue(cam)

    val stringPath = "/data/expected/cam_test_undirected"
    val expected = Source.fromFile(stringPath)

    assert(expected.mkString eq result)

    expected.close
  }
}
