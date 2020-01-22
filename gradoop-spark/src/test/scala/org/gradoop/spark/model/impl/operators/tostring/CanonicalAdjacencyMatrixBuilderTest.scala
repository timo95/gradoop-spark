package org.gradoop.spark.model.impl.operators.tostring

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.GraphCollection
import org.gradoop.spark.model.api.layouts.gve.GveGraphCollectionOperators
import org.gradoop.spark.model.impl.operators.tostring.gve.CanonicalAdjacencyMatrixBuilder
import org.gradoop.spark.model.impl.operators.tostring.gve.ElementToString._
import org.gradoop.spark.util.SparkAsciiGraphLoader
import org.gradoop.spark.{EpgmGradoopSparkTestBase, OperatorTest}

import scala.io.Source

class CanonicalAdjacencyMatrixBuilderTest extends EpgmGradoopSparkTestBase {
  val config: GradoopSparkConfig[L] = getConfig
  val gdlPath: String = getClass.getResource("/data/gdl/cam_test.gdl").getFile
  val loader: SparkAsciiGraphLoader[L] = SparkAsciiGraphLoader.fromFile(getConfig, gdlPath)
  val collection: GraphCollection[L#T] with GveGraphCollectionOperators[L#T] = loader.getGraphCollection

  describe("Directed") {
    val cam = new CanonicalAdjacencyMatrixBuilder[L](graphHeadToDataString,
      vertexToDataString, edgeToDataString,true)

    val result = collection.callForValue(cam)

    it("Equals expected string", OperatorTest) {
      val stringPath = getClass.getResource("/data/string/cam_test_directed").getFile
      val expected = Source.fromFile(stringPath)
      assert(expected.mkString equals result)
      expected.close
    }
  }

  describe("Undirected") {
    val cam = new CanonicalAdjacencyMatrixBuilder[L](graphHeadToDataString,
      vertexToDataString, edgeToDataString,false)

    val result = collection.callForValue(cam)

    it("Equals expected string", OperatorTest) {
      val stringPath = getClass.getResource("/data/string/cam_test_undirected").getFile
      val expected = Source.fromFile(stringPath)
      assert(expected.mkString equals result)
      expected.close
    }
  }
}
