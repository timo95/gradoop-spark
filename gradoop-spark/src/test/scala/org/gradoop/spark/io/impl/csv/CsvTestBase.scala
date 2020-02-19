package org.gradoop.spark.io.impl.csv

import org.gradoop.common.GradoopTestUtils
import org.gradoop.common.id.GradoopId
import org.gradoop.common.properties.PropertyValue
import org.gradoop.spark.EpgmGradoopSparkTestBase

trait CsvTestBase extends EpgmGradoopSparkTestBase {

  protected def getExtendedLogicalGraph: LGve#LG = {
    val factory = gveConfig.logicalGraphFactory
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

  protected def getLogicalGraphWithDelimiters: LGve#LG = {
    val string1 = "abc;,|:\n=\\ def"
    val string2 = "def;,|:\n=\\ ghi"
    val list = Seq(PropertyValue(string2), PropertyValue(string1))
    val set = list.toSet
    val map1 = Map(
      PropertyValue(string1) -> PropertyValue(string2),
      PropertyValue("key") -> PropertyValue(string1)
    )
    val map2 = Map(
      PropertyValue(string1) -> PropertyValue(1),
      PropertyValue("key") -> PropertyValue(2)
    )
    val map3 = Map(
      PropertyValue(1) -> PropertyValue(string2),
      PropertyValue(2) -> PropertyValue(string1)
    )
    val props = Map(
      string1 -> PropertyValue(string2),
      string2 -> PropertyValue(true),
      "key3" -> PropertyValue(string2),
      "key4" -> PropertyValue(list),
      "key5" -> PropertyValue(set),
      "key6" -> PropertyValue(map1),
      "key6" -> PropertyValue(map2),
      "key6" -> PropertyValue(map3)
    )

    val config = gveConfig
    val factory = config.logicalGraphFactory
    import config.sparkSession.implicits._

    val graphHead = factory.graphHeadFactory.create(string1, props)
    val graphHeads = factory.createDataset(Seq(graphHead))
    val vertex = factory.vertexFactory.create(string1, props, Set(graphHead.id))
    val vertices = factory.createDataset(Seq(vertex))
    val edge = factory.edgeFactory.create(string1, vertex.id, vertex.id, props, Set(graphHead.id))
    val edges = factory.createDataset(Seq(edge))

    factory.init(graphHeads, vertices, edges)
  }
}
