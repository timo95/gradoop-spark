package org.gradoop.spark.io.impl.csv.indexed

import org.gradoop.spark.util.SparkAsciiGraphLoader
import org.gradoop.spark.{EpgmGradoopSparkTestBase, IoTest}

class IndexedCsvDataSourceTest extends EpgmGradoopSparkTestBase {

  it("Read indexed graph collection", IoTest) {
    val csvPath = getClass.getResource("/data/csv/input_indexed_graph_collection").getFile
    val gdlPath = getClass.getResource("/data/gdl/csv_source_expected/expected_graph_collection.gdl").getFile

    val dataSource = new IndexedCsvDataSource(csvPath, tflConfig)
    val graph = dataSource.readGraphCollection

    val expected = SparkAsciiGraphLoader.fromFile(gveConfig, gdlPath)
      .getGraphCollectionByVariables("expected1", "expected2")

    assert(graph.asGve(gveConfig).equalsByGraphData(expected))
  }

  it("Read indexed logical graph", IoTest) {
    val csvPath = getClass.getResource("/data/csv/input_indexed").getFile
    val gdlPath = getClass.getResource("/data/gdl/csv_source_expected/expected.gdl").getFile

    val dataSource = new IndexedCsvDataSource(csvPath, tflConfig)
    val graph = dataSource.readLogicalGraph

    val loader = SparkAsciiGraphLoader.fromFile(gveConfig, gdlPath)
    val expected = loader.getLogicalGraphByVariable("expected")

    assert(graph.asGve(gveConfig).equalsByData(expected))
  }

  it("Read indexed graph collection with empty edges", IoTest) {
    val csvPath = getClass.getResource("/data/csv/input_indexed_graph_collection_no_edges").getFile
    val gdlPath = getClass.getResource("/data/gdl/csv_source_expected/expected_no_edges.gdl").getFile

    val dataSource = new IndexedCsvDataSource(csvPath, tflConfig)
    val graph = dataSource.readGraphCollection

    val expected = SparkAsciiGraphLoader.fromFile(gveConfig, gdlPath)
      .getGraphCollectionByVariables("expected1", "expected2")

    assert(graph.asGve(gveConfig).equalsByGraphData(expected))
  }
}
