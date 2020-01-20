package org.gradoop.spark.io.impl.csv

import org.gradoop.common.GradoopTestUtils
import org.gradoop.common.id.GradoopId
import org.gradoop.spark.EpgmGradoopSparkTestBase
import org.gradoop.spark.model.api.layouts.gve.GveBaseLayoutFactory
import org.gradoop.spark.util.SparkAsciiGraphLoader

class CsvDataSourceTest extends EpgmGradoopSparkTestBase {

  describe("csv data source") {
    val config = getConfig

    it("correctly reads extended properties logical graph") {
      val csvPath = getClass.getResource("/data/csv/input_extended_properties").getFile
      val csvDataSource = CsvDataSource(csvPath, config)
      val graph = csvDataSource.readLogicalGraph

      val expected = getExtendedLogicalGraph(config.logicalGraphFactory)

      assert(graph.equalsByData(expected))
    }

    it("correctly reads graph collection") {
      val csvPath = getClass.getResource("/data/csv/input_graph_collection").getFile
      val csvDataSource = CsvDataSource(csvPath, config)
      val collection = csvDataSource.readGraphCollection

      val gdlPath = getClass.getResource("/data/gdl/csv_source_expected/expected_graph_collection.gdl").getFile
      val expected = SparkAsciiGraphLoader.fromFile(config, gdlPath)
        .getGraphCollectionByVariables("expected1", "expected2")

      assert(collection.equalsByGraphData(expected))
    }
  }

  protected def getExtendedLogicalGraph(factory: GveBaseLayoutFactory[L, L#LG]): L#LG = {
    import factory.Implicits._

    val idUser = GradoopId.get
    val idPost = GradoopId.get
    val idForum = GradoopId.get
    val graphIds = Set(idForum)
    val properties = GradoopTestUtils.PROPERTIES
    val graphHead = factory.createDataset(Seq(factory.graphHeadFactory(idForum, "Forum", properties)))
    val vertices = factory.createDataset(Seq(
      factory.vertexFactory(idUser, "User", properties, graphIds),
      factory.vertexFactory(idPost, "Post", properties, graphIds)))
    val edges = factory.createDataset(Seq(
      factory.edgeFactory(GradoopId.get, "creatorOf", idUser, idPost, properties, graphIds)))
    factory.init(graphHead, vertices, edges)
  }
}
