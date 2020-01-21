package org.gradoop.spark.io.impl.csv

import java.nio.file.Files

import org.apache.spark.sql.{Dataset, SaveMode}
import org.gradoop.spark.EpgmGradoopSparkTestBase
import org.gradoop.spark.io.impl.metadata.ElementMetaData

class CsvMetaDataSinkTest extends EpgmGradoopSparkTestBase {

  describe("CsvMetaDataSink") {
    val tempDir = Files.createTempDirectory("csv_metadata").toString

    it("GraphCollection") {
      val path = getClass.getResource("/data/csv/input_graph_collection").getFile
      val metaData = CsvMetaDataSource(path).read

      CsvMetaDataSink(tempDir).write(metaData, SaveMode.Overwrite)
      val writtenMetaData = CsvMetaDataSource(tempDir).read

      assert(metaDataEquals(metaData.graphHeadMetaData, writtenMetaData.graphHeadMetaData))
      assert(metaDataEquals(metaData.vertexMetaData, writtenMetaData.vertexMetaData))
      assert(metaDataEquals(metaData.edgeMetaData, writtenMetaData.edgeMetaData))
    }

    it("LogicalGraph with extended properties") {
      val path = getClass.getResource("/data/csv/input_extended_properties").getFile
      val metaData = CsvMetaDataSource(path).read

      CsvMetaDataSink(tempDir).write(metaData, SaveMode.Overwrite)
      val writtenMetaData = CsvMetaDataSource(tempDir).read

      assert(metaDataEquals(metaData.graphHeadMetaData, writtenMetaData.graphHeadMetaData))
      assert(metaDataEquals(metaData.vertexMetaData, writtenMetaData.vertexMetaData))
      assert(metaDataEquals(metaData.edgeMetaData, writtenMetaData.edgeMetaData))
    }

    def metaDataEquals(left: Dataset[ElementMetaData], right: Dataset[ElementMetaData]): Boolean = {
      left.collect.sortBy(_.label).sameElements(right.collect.sortBy(_.label))
    }
  }
}
