package org.gradoop.spark.io.impl.metadata

import org.gradoop.spark.EpgmGradoopSparkTestBase

class MetaDataTest extends EpgmGradoopSparkTestBase {

  describe("MetaData") {
    it("can be created from a graph") {
      val loader = getSocialNetworkLoader
      val graph = loader.logicalGraph

      MetaData(graph)
    }
  }
}
