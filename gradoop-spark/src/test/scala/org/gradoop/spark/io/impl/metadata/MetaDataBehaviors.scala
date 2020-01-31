package org.gradoop.spark.io.impl.metadata

import org.gradoop.common.util.Type
import org.gradoop.spark.EpgmGradoopSparkTestBase
import org.gradoop.spark.model.impl.types.EpgmGve
import org.scalatest.FunSpec

trait MetaDataBehaviors extends EpgmGradoopSparkTestBase {
  this: FunSpec =>

  def createMetaData(createMetaData: EpgmGve#LG => MetaData) {
    it("Social Network Graph") {
      val graph = getSocialNetworkLoader.getLogicalGraph

      val expectedG = Seq(ElementMetaData("_DB", Seq.empty))

      val expectedV = Seq(
        ElementMetaData("Tag", Seq(PropertyMetaData("name", Type.STRING))),
        ElementMetaData("Forum", Seq(PropertyMetaData("title", Type.STRING))),
        ElementMetaData("Person", Seq(
          PropertyMetaData("name", Type.STRING),
          PropertyMetaData("gender", Type.STRING),
          PropertyMetaData("city", Type.STRING),
          PropertyMetaData("age", Type.INTEGER),
          PropertyMetaData("speaks", Type.STRING),
          PropertyMetaData("locIP", Type.STRING)
        ))
      )

      val expectedE = Seq(
        ElementMetaData("hasInterest", Seq.empty),
        ElementMetaData("hasModerator", Seq(PropertyMetaData("since", Type.INTEGER))),
        ElementMetaData("hasMember", Seq.empty),
        ElementMetaData("hasTag", Seq.empty),
        ElementMetaData("knows", Seq(PropertyMetaData("since", Type.INTEGER)))
      )

      val metaData = createMetaData(graph)

      assert(metaDataEquals(metaData.graphHeadMetaData.collect, expectedG))
      assert(metaDataEquals(metaData.vertexMetaData.collect, expectedV))
      assert(metaDataEquals(metaData.edgeMetaData.collect, expectedE))
    }
  }

  private def metaDataEquals(left: Seq[ElementMetaData], right: Seq[ElementMetaData]): Boolean = {
    val sortedLeft = left.sortBy(_.label)
      .map(m => ElementMetaData(m.label, m.metaData.sortBy(_.key)))
    val sortedRight = right.sortBy(_.label)
      .map(m => ElementMetaData(m.label, m.metaData.sortBy(_.key)))

    sortedLeft == sortedRight
  }
}
