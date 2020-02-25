package org.gradoop.spark.model.impl.operators.tostring

import org.gradoop.spark.model.api.graph.GraphCollection
import org.gradoop.spark.model.api.layouts.gve.GveGraphCollectionOperators
import org.gradoop.spark.model.impl.operators.tostring.gve.CanonicalAdjacencyMatrixBuilder
import org.gradoop.spark.model.impl.operators.tostring.gve.ElementToString._
import org.gradoop.spark.util.SparkAsciiGraphLoader
import org.gradoop.spark.{EpgmGradoopSparkTestBase, OperatorTest}

import scala.io.Source

class CanonicalAdjacencyMatrixBuilderTest extends EpgmGradoopSparkTestBase {

  describe("CanonicalAdjacencyMatrixBuilder test") {
    val gdlPath: String = getClass.getResource("/data/gdl/cam_test.gdl").getFile

    it("Directed", OperatorTest) {
      val loader: SparkAsciiGraphLoader[LGve] = SparkAsciiGraphLoader.fromFile(gveConfig, gdlPath)
      val collection: GraphCollection[LGve#T] with GveGraphCollectionOperators[LGve#T] = loader.getGraphCollection

      val cam = new CanonicalAdjacencyMatrixBuilder[LGve](graphHeadToDataString,
        vertexToDataString, edgeToDataString,true)
      val result = collection.callForValue(cam)
      val stringPath = getClass.getResource("/data/string/cam_test_directed").getFile
      val expected = Source.fromFile(stringPath)
      assert(expected.mkString equals result)
      expected.close
    }

    it("Undirected", OperatorTest) {
      val loader: SparkAsciiGraphLoader[LGve] = SparkAsciiGraphLoader.fromFile(gveConfig, gdlPath)
      val collection: GraphCollection[LGve#T] with GveGraphCollectionOperators[LGve#T] = loader.getGraphCollection

      val cam = new CanonicalAdjacencyMatrixBuilder[LGve](graphHeadToDataString,
        vertexToDataString, edgeToDataString,false)
      val result = collection.callForValue(cam)
      val stringPath = getClass.getResource("/data/string/cam_test_undirected").getFile
      val expected = Source.fromFile(stringPath)
      assert(expected.mkString equals result)
      expected.close
    }
  }
}
