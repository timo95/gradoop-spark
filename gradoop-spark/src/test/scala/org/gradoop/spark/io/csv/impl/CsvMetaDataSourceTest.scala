package org.gradoop.spark.io.csv.impl

import org.gradoop.spark.EpgmGradoopSparkTestBase
import org.gradoop.spark.io.impl.csv.CsvMetaDataSource
import org.gradoop.spark.io.impl.metadata.PropertyMetaData

class CsvMetaDataSourceTest extends EpgmGradoopSparkTestBase {
  private val config = getConfig

  describe("graph collection metadata") {
    val path = getClass.getResource("/data/csv/input_graph_collection").getFile
    val metaDataSource = new CsvMetaDataSource(path, config)

    val metaData = metaDataSource.read

    val graphHeadMetaData = metaData.graphHeadMetaData.collect()
    val vertexMetaData = metaData.vertexMetaData.collect()
    val edgeMetaData = metaData.edgeMetaData.collect()

    it("correct number of entries") {
      assert(graphHeadMetaData.length == 2)
      assert(vertexMetaData.length == 2)
      assert(edgeMetaData.length == 2)
    }
    it("correct labels") {
      assert(graphHeadMetaData.map(m => m.label).toSet.equals(Set("g1", "g2")))
      assert(vertexMetaData.map(m => m.label).toSet.equals(Set("A", "B")))
      assert(edgeMetaData.map(m => m.label).toSet.equals(Set("a", "b")))
    }
    it("correct number of properties") {
      assert(graphHeadMetaData.map(m => m.properties.length).toSet.equals(Set(2, 2)))
      assert(vertexMetaData.map(m => m.properties.length).toSet.equals(Set(4, 3)))
      assert(edgeMetaData.map(m => m.properties.length).toSet.equals(Set(2, 1)))
    }
    it("correct properties") {
      val propsG1 = Array(PropertyMetaData("a", "string"), PropertyMetaData("b", "double"))
      val propsG2 = Array(PropertyMetaData("a", "string"), PropertyMetaData("b", "int"))
      assert(graphHeadMetaData.map(m => m.properties.deep).toSet.equals(Set(propsG1.deep, propsG2.deep)))

      val propsV1 = Array(PropertyMetaData("a", "string"), PropertyMetaData("b", "int"), PropertyMetaData("c", "float"), PropertyMetaData("d", "null"))
      val propsV2 = Array(PropertyMetaData("a", "long"), PropertyMetaData("b", "boolean"), PropertyMetaData("c", "double"))
      assert(vertexMetaData.map(m => m.properties.deep).toSet.equals(Set(propsV1.deep, propsV2.deep)))

      val propsE1 = Array(PropertyMetaData("a", "int"), PropertyMetaData("b", "float"))
      val propsE2 = Array(PropertyMetaData("a", "long"))
      assert(edgeMetaData.map(m => m.properties.deep).toSet.equals(Set(propsE1.deep, propsE2.deep)))
    }
  }
}
