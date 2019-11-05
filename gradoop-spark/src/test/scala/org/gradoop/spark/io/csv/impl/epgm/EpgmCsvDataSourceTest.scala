package org.gradoop.spark.io.csv.impl.epgm

import org.gradoop.common.model.impl.id.GradoopId
import org.gradoop.spark.EpgmGradoopSparkTestBase
import org.gradoop.spark.io.impl.csv.epgm.EpgmCsvDataSource

class EpgmCsvDataSourceTest extends EpgmGradoopSparkTestBase {
  private val config = getConfig

  describe("logical graph with extended properties") {
    val csvDataSource = EpgmCsvDataSource(getClass.getResource("/data/csv/input_extended_properties").getFile, config)
    val graph = csvDataSource.getLogicalGraph

    it("correct number of elements") {
      assert(graph.getGraphHead.count() == 1)
      assert(graph.getVertices.count() == 2)
      assert(graph.getEdges.count() == 1)
    }
    it("correct graphHead id") {
      assert(graph.getGraphHead.collect()(0).getId == GradoopId.fromString("000000000000000000000000"))
    }
    it("correct vertex ids") {
      val vertexIds = graph.getVertices.collect.map(v => v.getId).toSet
      val expected = Set[String]("000000000000000000000000", "000000000000000000000001")
        .map(GradoopId.fromString)
      assert(vertexIds == expected)
    }
    it("correct edge id") {
      assert(graph.getEdges.collect()(0).getId == GradoopId.fromString("000000000000000000000002"))
    }
  }

  describe("graph collection") {
    val csvDataSource = EpgmCsvDataSource(getClass.getResource("/data/csv/input_graph_collection").getFile, config)
    val collection = csvDataSource.getGraphCollection

    it("correct number of elements") {
      assert(collection.getGraphHeads.count() == 2)
      assert(collection.getVertices.count() == 5)
      assert(collection.getEdges.count() == 6)
    }
  }


}
