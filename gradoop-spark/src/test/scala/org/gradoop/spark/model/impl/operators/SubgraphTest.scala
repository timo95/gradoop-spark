package org.gradoop.spark.model.impl.operators

class SubgraphTest extends EpgmGradoopSparkTestBase {

  "A subgraph of Social Network" should "be as expected" in {
    val loader = getSocialNetworkLoader
    val logicalGraph = loader.getLogicalGraph
  }

}
