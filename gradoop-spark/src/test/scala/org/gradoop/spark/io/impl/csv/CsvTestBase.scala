package org.gradoop.spark.io.impl.csv

import org.gradoop.common.GradoopTestUtils
import org.gradoop.common.id.GradoopId
import org.gradoop.spark.EpgmGradoopSparkTestBase
import org.gradoop.spark.model.api.layouts.gve.GveBaseLayoutFactory

class CsvTestBase extends EpgmGradoopSparkTestBase {

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
