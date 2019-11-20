package org.gradoop.spark.io.csv.impl

import org.gradoop.spark.EpgmGradoopSparkTestBase
import org.gradoop.spark.io.impl.csv.CsvDataSource

class CsvDataSourceTest extends EpgmGradoopSparkTestBase {
  private val config = getConfig
  import config.implicits._

  describe("logical graph with extended properties") {
    val path = getClass.getResource("/data/csv/input_extended_properties").getFile
    val csvDataSource = CsvDataSource(path, config)
    val graph = csvDataSource.readLogicalGraph

    it("correct number of elements") {
      assert(graph.graphHead.count() == 1)
      assert(graph.vertices.count() == 2)
      assert(graph.edges.count() == 1)
    }
    it("correct ids") {
      assert(graph.graphHead.collect()(0).id.toString == "000000000000000000000000")

      val expected = Set[String]("000000000000000000000000", "000000000000000000000001")
      assert(graph.vertices.collect.map(v => v.id.toString).toSet == expected)

      assert(graph.edges.collect()(0).id.toString == "000000000000000000000002")
    }
    it("correct labels") {
      assert(graph.graphHead.collect()(0).label == "Forum")

      val expected = Set[String]("User", "Post")
      assert(graph.vertices.collect.map(v => v.label).toSet == expected)

      assert(graph.edges.collect()(0).label == "creatorOf")
    }
  }

  describe("graph collection") {
    val csvDataSource = CsvDataSource(getClass.getResource("/data/csv/input_graph_collection").getFile, config)
    val collection = csvDataSource.readGraphCollection

    it("correct number of elements") {
      assert(collection.graphHeads.count() == 2)
      assert(collection.vertices.count() == 5)
      assert(collection.edges.count() == 6)
    }
  }
}
