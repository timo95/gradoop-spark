package org.gradoop.spark.io.impl.csv

import java.nio.file.Files

import org.apache.spark.sql.SaveMode
import org.gradoop.spark.IoTest
import org.gradoop.spark.util.SparkAsciiGraphLoader

class CsvDataSinkTest extends CsvTestBase {
  private val tempDir = Files.createTempDirectory("csv").toString

  describe("CsvDataSink") {
    val config = getConfig

    it("social network graph collection", IoTest) {
      testCsvWrite(getSocialNetworkLoader.getGraphCollection)
    }

    it("logical graph with different property types for the same property name", IoTest) {
      val loader = SparkAsciiGraphLoader.fromString(config, "vertices[" +
        "(v1:A {keya:1, keyb:2, keyc:\"Foo\"})" +
        "(v2:A {keya:1.2f, keyb:\"Bar\", keyc:2.3f})" +
        "(v3:A {keya:\"Bar\", keyb:true})]" +
        "edges[" +
        "(v1)-[e1:a {keya:14, keyb:3, keyc:\"Foo\"}]->(v1)" +
        "(v1)-[e2:a {keya:1.1f, keyb:\"Bar\", keyc:2.5f}]->(v1)" +
        "(v1)-[e3:a {keya:true, keyb:3.13f}]->(v1)]"
      )

      testCsvWrite(loader.getLogicalGraphByVariable("vertices"))
      testCsvWrite(loader.getLogicalGraphByVariable("edges"))
    }

    it("logical graph with the same label for vertices and edges", IoTest) {
      val loader = SparkAsciiGraphLoader.fromString(config, "single[" +
        "(v1:A {keya:2})" +
        "(v1)-[e1:A {keya:false}]->(v1)]" +
        "multiple[" +
        "(v2:B {keya:true, keyb:1, keyc:\"Foo\"})" +
        "(v3:B {keya:false, keyb:2})" +
        "(v4:C {keya:2.3f, keyb:\"Bar\"})" +
        "(v5:C {keya:1.1f})" +
        "(v2)-[e2:B {keya:1, keyb:2.23d, keyc:3.3d}]->(v3)" +
        "(v3)-[e3:B {keya:2, keyb:7.2d}]->(v2)" +
        "(v4)-[e4:C {keya:false}]->(v4)" +
        "(v5)-[e5:C {keya:true, keyb:13}]->(v5)]")

      testCsvWrite(loader.getLogicalGraphByVariable("single"))
      testCsvWrite(loader.getLogicalGraphByVariable("multiple"))    }

    it("logical graph with extended properties", IoTest) {
      testCsvWrite(getExtendedLogicalGraph)
    }

    it("logical graph containing delimiters", IoTest) {
      testCsvWrite(getLogicalGraphWithDelimiters)
    }
  }

  private def testCsvWrite(graph: LGve#LG): Unit = {
    CsvDataSink(tempDir, getConfig).write(graph, SaveMode.Overwrite)
    val writtenGraph = CsvDataSource(tempDir, getConfig).readLogicalGraph
    assert(graph.equalsByData(writtenGraph))
  }

  private def testCsvWrite(collection: LGve#GC): Unit = {
    CsvDataSink(tempDir, getConfig).write(collection, SaveMode.Overwrite)
    val writtenCollection = CsvDataSource(tempDir, getConfig).readGraphCollection
    assert(collection.equalsByGraphData(writtenCollection))
  }
}
