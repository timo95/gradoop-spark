package org.gradoop.spark.io.impl.csv

import org.gradoop.spark.util.SparkAsciiGraphLoader

class CsvDataSourceTest extends CsvTestBase {

  describe("CsvDataSource") {
    val config = getConfig

    it("reads logical graph with extended properties") {
      val csvPath = getClass.getResource("/data/csv/input_extended_properties").getFile
      val csvDataSource = CsvDataSource(csvPath, config)
      val graph = csvDataSource.readLogicalGraph

      val expected = getExtendedLogicalGraph(config.logicalGraphFactory)

      assert(graph.equalsByData(expected))
    }

    it("reads graph collection") {
      val csvPath = getClass.getResource("/data/csv/input_graph_collection").getFile
      val csvDataSource = CsvDataSource(csvPath, config)
      val collection = csvDataSource.readGraphCollection

      val gdlPath = getClass.getResource("/data/gdl/csv_source_expected/expected_graph_collection.gdl").getFile
      val expected = SparkAsciiGraphLoader.fromFile(config, gdlPath)
        .getGraphCollectionByVariables("expected1", "expected2")

      assert(collection.equalsByGraphData(expected))
    }
  }
}
