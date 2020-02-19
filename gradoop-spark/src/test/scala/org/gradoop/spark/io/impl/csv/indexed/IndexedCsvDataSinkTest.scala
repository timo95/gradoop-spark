package org.gradoop.spark.io.impl.csv.indexed

import java.nio.file.Files

import org.apache.spark.sql.SaveMode
import org.gradoop.spark.IoTest
import org.gradoop.spark.io.impl.csv.CsvTestBase
import org.gradoop.spark.model.impl.types.Tfl
import org.gradoop.spark.transformation.TransformationFunctions
import org.gradoop.spark.util.SparkAsciiGraphLoader

class IndexedCsvDataSinkTest extends CsvTestBase {
  private val tempDir = Files.createTempDirectory("csv").toString

  it("graph collection", IoTest) {
    val input = getSocialNetworkLoader.getGraphCollection
    testIndexedCsvWrite(input.asTfl(tflConfig))
  }

  it("logical graph", IoTest) {
    val input = getSocialNetworkLoader.getLogicalGraph
    testIndexedCsvWrite(input.asTfl(tflConfig))
  }

  it("logical graph with different property types", IoTest) {
    val loader = SparkAsciiGraphLoader.fromString(gveConfig, "g:graph1[" +
      "(v1:A {keya:1, keyb:2, keyc:\"Foo\"})" +
      "(v2:A {keya:1.2f, keyb:\"Bar\", keyc:2.3f})" +
      "(v3:A {keya:\"Bar\", keyb:true})" +
      "(v1)-[e1:a {keya:14, keyb:3, keyc:\"Foo\"}]->(v1)" +
      "(v1)-[e2:a {keya:1.1f, keyb:\"Bar\", keyc:2.5f}]->(v1)" +
      "(v1)-[e3:a {keya:true, keyb:3.13f}]->(v1)]")

    testIndexedCsvWrite(loader.getLogicalGraphByVariable("g").asTfl(tflConfig))
  }

  it("graph collection with same label", IoTest) {
    val loader = SparkAsciiGraphLoader.fromString(gveConfig, "single:graph1[" +
      "(v1:B {keya:2})" +
      "(v1)-[e1:A {keya:false}]->(v1)," +
      "]" +
      "multiple:graph2[" +
      "(v2:B {keya:true, keyb:1, keyc:\"Foo\"})," +
      "(v3:B {keya:false, keyb:2})," +
      "(v4:C {keya:2.3f, keyb:\"Bar\"})," +
      "(v5:C {keya:1.1f})," +
      "(v2)-[e2:B {keya:1, keyb:2.23d, keyc:3.3d}]->(v3)," +
      "(v3)-[e3:B {keya:2, keyb:7.2d}]->(v2)," +
      "(v4)-[e4:C {keya:false}]->(v4)," +
      "(v5)-[e5:C {keya:true, keyb:13}]->(v5)" +
      "]")

    testIndexedCsvWrite(loader.getGraphCollectionByVariables("single", "multiple").asTfl(tflConfig))
  }

  it("graph collection with different labels in same file", IoTest) {
    // Test correct behavior when different labels result in the same directory name
    val fac = gveConfig.graphCollectionFactory
    import fac.Implicits._

    // This results in the path "graphs/a_b" because < and > are illegal filename characters.
    val graphHead1 = fac.graphHeadFactory.create("a<b")
    val graphHead2 = fac.graphHeadFactory.create("a>b")
    val graphHeads = Seq(graphHead1, graphHead2)

    // This results in the path "vertices/b_c" because < and > are illegal filename characters.
    val vertex1 = fac.vertexFactory.create("B<C")
    val vertex2 = fac.vertexFactory.create("B>C")
    val vertices = Seq(vertex1, vertex2)

    val edge1 = fac.edgeFactory.create("c<d", vertex1.id, vertex2.id)
    val edge2 = fac.edgeFactory.create("c>d", vertex2.id, vertex1.id)
    val edges = Seq(edge1, edge2)

    val collection = fac.init(graphHeads, vertices, edges)
      .transformVertices(TransformationFunctions.addGraphIds(Iterable(graphHead1.id, graphHead2.id)))
      .transformEdges(TransformationFunctions.addGraphId(graphHead1.id))
    testIndexedCsvWrite[LTfl](collection.asTfl(tflConfig))
  }

  it("logical graph with delimiter characters", IoTest) {
    testIndexedCsvWrite(getLogicalGraphWithDelimiters.asTfl(tflConfig))
  }

  it("graph collection with empty label", IoTest) {
    val loader = SparkAsciiGraphLoader.fromString(gveConfig, "g0[" +
      "(v1 {keya:2})" +
      "(v2)" +
      "(v3:_)" +
      "]" +
      "g1 {key:\"property\"}[" +
      "(v4)-[e1 {keya:1}]->(v4)" +
      "(v4)-[e2]->(v4)" +
      "(v4)-[e3:_ {keya:false}]->(v4)" +
      "]" +
      "g2:_ {graph:\"hasALabel\"}[" +
      "(v5)" +
      "]")

    testIndexedCsvWrite(loader.getGraphCollectionByVariables("g0", "g1", "g2").asTfl(tflConfig))
  }

  private def testIndexedCsvWrite[L <: Tfl[L]](graph: L#LG): Unit = {
    IndexedCsvDataSink(tempDir, graph.config).write(graph, SaveMode.Overwrite)
    val writtenGraph = IndexedCsvDataSource(tempDir, graph.config).readLogicalGraph
    assert(writtenGraph.asGve(gveConfig).equalsByData(graph.asGve(gveConfig)))
  }

  private def testIndexedCsvWrite[L <: Tfl[L]](collection: L#GC): Unit = {
    IndexedCsvDataSink(tempDir, collection.config).write(collection, SaveMode.Overwrite)
    val writtenCollection = IndexedCsvDataSource(tempDir, collection.config).readGraphCollection
    assert(writtenCollection.asGve(gveConfig).equalsByGraphData(collection.asGve(gveConfig)))
  }
}
