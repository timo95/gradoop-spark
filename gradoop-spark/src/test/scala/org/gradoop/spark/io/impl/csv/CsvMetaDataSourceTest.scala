package org.gradoop.spark.io.impl.csv

import org.gradoop.spark.io.impl.metadata.PropertyMetaData
import org.gradoop.spark.{EpgmGradoopSparkTestBase, IoTest}

class CsvMetaDataSourceTest extends EpgmGradoopSparkTestBase {

  describe("CsvMetaDataSource - graph collection") {
    val path = getClass.getResource("/data/csv/input_graph_collection").getFile
    val metaDataSource = CsvMetaDataSource(path)

    val metaData = metaDataSource.read

    val graphHeadMetaData = metaData.graphHeadMetaData.collect()
    val vertexMetaData = metaData.vertexMetaData.collect()
    val edgeMetaData = metaData.edgeMetaData.collect()

    it("correct number of entries", IoTest) {
      assert(graphHeadMetaData.length == 2)
      assert(vertexMetaData.length == 2)
      assert(edgeMetaData.length == 2)
    }
    it("correct labels", IoTest) {
      assert(graphHeadMetaData.map(_.label).sorted.sameElements(Array("g1", "g2")))
      assert(vertexMetaData.map(_.label).sorted.sameElements(Array("A", "B")))
      assert(edgeMetaData.map(_.label).sorted.sameElements(Array("a", "b")))
    }
    it("correct number of properties", IoTest) {
      assert(graphHeadMetaData.sortBy(_.label).map(_.metaData.length).sameElements(Array(2, 2)))
      assert(vertexMetaData.sortBy(_.label).map(_.metaData.length).sameElements(Array(4, 3)))
      assert(edgeMetaData.sortBy(_.label).map(_.metaData.length).sameElements(Array(2, 1)))
    }
    it("correct properties", IoTest) {
      val propsG1 = Array(PropertyMetaData("a", "string"), PropertyMetaData("b", "double"))
      val propsG2 = Array(PropertyMetaData("a", "string"), PropertyMetaData("b", "int"))
      assert(graphHeadMetaData.sortBy(_.label).map(_.metaData).sameElements(Array(propsG1.deep, propsG2.deep)))

      val propsV1 = Array(
        PropertyMetaData("a", "string"),
        PropertyMetaData("b", "int"),
        PropertyMetaData("c", "float"),
        PropertyMetaData("d", "null"))
      val propsV2 = Array(
        PropertyMetaData("a", "long"),
        PropertyMetaData("b", "boolean"),
        PropertyMetaData("c", "double"))
      assert(vertexMetaData.sortBy(_.label).map(_.metaData).sameElements(Array(propsV1.deep, propsV2.deep)))

      val propsE1 = Array(PropertyMetaData("a", "int"), PropertyMetaData("b", "float"))
      val propsE2 = Array(PropertyMetaData("a", "long"))
      assert(edgeMetaData.sortBy(_.label).map(_.metaData).sameElements(Array(propsE1.deep, propsE2.deep)))
    }
  }

  describe("CsvMetaDataSource - logical graph with extended properties") {
    val path = getClass.getResource("/data/csv/input_extended_properties").getFile
    val metaDataSource = CsvMetaDataSource(path)

    val metaData = metaDataSource.read

    val graphHeadMetaData = metaData.graphHeadMetaData.collect()
    val vertexMetaData = metaData.vertexMetaData.collect()
    val edgeMetaData = metaData.edgeMetaData.collect()

    it("correct number of entries", IoTest) {
      assert(graphHeadMetaData.length == 1)
      assert(vertexMetaData.length == 2)
      assert(edgeMetaData.length == 1)
    }
    it("correct labels", IoTest) {
      assert(graphHeadMetaData.map(_.label).sorted.sameElements(Array("Forum")))
      assert(vertexMetaData.map(_.label).sorted.sameElements(Array("Post", "User")))
      assert(edgeMetaData.map(_.label).sorted.sameElements(Array("creatorOf")))
    }
    it("correct number of properties", IoTest) {
      assert(graphHeadMetaData.sortBy(_.label).map(_.metaData.length).sameElements(Array(18)))
      assert(vertexMetaData.sortBy(_.label).map(_.metaData.length).sameElements(Array(18, 18)))
      assert(edgeMetaData.sortBy(_.label).map(_.metaData.length).sameElements(Array(18)))
    }
    it("correct properties", IoTest) {
      val props = Array(
        PropertyMetaData("key0", "boolean"),
        PropertyMetaData("key1", "int"),
        PropertyMetaData("key2", "long"),
        PropertyMetaData("key3", "float"),
        PropertyMetaData("key4", "double"),
        PropertyMetaData("key5", "string"),
        PropertyMetaData("key6", "gradoopid"),
        PropertyMetaData("key7", "localdate"),
        PropertyMetaData("key8", "localtime"),
        PropertyMetaData("key9", "localdatetime"),
        PropertyMetaData("keya", "bigdecimal"),
        PropertyMetaData("keyb", "map:string:double"),
        PropertyMetaData("keyc", "list:string"),
        PropertyMetaData("keyd", "list:int"),
        PropertyMetaData("keye", "short"),
        PropertyMetaData("keyf", "null"),
        PropertyMetaData("keyg", "set:string"),
        PropertyMetaData("keyh", "set:int")
      )

      assert(graphHeadMetaData.sortBy(_.label).map(_.metaData).sameElements(Array(props.deep)))
      assert(vertexMetaData.sortBy(_.label).map(_.metaData).sameElements(Array(props.deep, props.deep)))
      assert(edgeMetaData.sortBy(_.label).map(_.metaData).sameElements(Array(props.deep)))
    }
  }
}
