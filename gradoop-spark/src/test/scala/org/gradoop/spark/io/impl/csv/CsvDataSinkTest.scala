package org.gradoop.spark.io.impl.csv

import java.nio.file.Files

import org.apache.spark.sql.SaveMode
import org.gradoop.spark.model.impl.operators.tostring.gve.{CanonicalAdjacencyMatrixBuilder, ElementToString}

class CsvDataSinkTest extends CsvTestBase {

  describe("CsvDataSink") {
    val config = getConfig
    val tempDir = Files.createTempDirectory("csv").toFile.getPath

    it("writes logical graph with extended properties") {
      val graph = getExtendedLogicalGraph(config.logicalGraphFactory)

      CsvDataSink(tempDir, getConfig).write(graph, SaveMode.Overwrite)

      val writtenGraph = CsvDataSource(tempDir, getConfig).readLogicalGraph

      val matrixBuilder = new CanonicalAdjacencyMatrixBuilder[L](ElementToString.graphHeadToDataString, ElementToString.vertexToDataString,
        ElementToString.edgeToDataString, true)

      println(matrixBuilder.execute(graph))
      println(matrixBuilder.execute(writtenGraph))

      assert(graph.equalsByData(writtenGraph))
    }
  }
}
