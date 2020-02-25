package org.gradoop.spark.io.impl.csv

import org.gradoop.spark.IoTest
import org.gradoop.spark.util.SparkAsciiGraphLoader

class CsvDataSourceTest extends CsvTestBase {

  describe("CsvDataSource") {
    it("Read logical graph with extended properties", IoTest) {
      val csvPath = getClass.getResource("/data/csv/input_extended_properties").getFile
      val csvDataSource = CsvDataSource(csvPath, gveConfig)
      val graph = csvDataSource.readLogicalGraph

      assert(graph.equalsByData(getExtendedLogicalGraph))
    }

    it("Read graph collection", IoTest) {
      val csvPath = getClass.getResource("/data/csv/input_graph_collection").getFile
      val csvDataSource = CsvDataSource(csvPath, gveConfig)
      val collection = csvDataSource.readGraphCollection

      val gdlPath = getClass.getResource("/data/gdl/csv_source_expected/expected_graph_collection.gdl").getFile
      val expected = SparkAsciiGraphLoader.fromFile(gveConfig, gdlPath)
        .getGraphCollectionByVariables("expected1", "expected2")

      assert(collection.equalsByGraphData(expected))
    }
  }
}
