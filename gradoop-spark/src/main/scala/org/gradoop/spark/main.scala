package org.gradoop.spark

import org.apache.spark.sql.SparkSession
import org.gradoop.common.model.impl.id.GradoopId
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.elements.{EpgmEdge, EpgmGraphHead, EpgmVertex}
import org.gradoop.spark.model.impl.types.EpgmShortcuts

object main extends EpgmShortcuts {
  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder
      .appName("Simple Application")
      .master("local[4]")
      .getOrCreate()

    val config = GradoopSparkConfig[G, V, E, LG, GC](GVE, GVE)

    //var prop1 = PropertyValue.create("value1")
    //var prop2 = PropertyValue.create(3)
    //var prop3 = PropertyValue.create(12.6)

    //var properties = new Properties()
    //properties.set("key1", prop1)
    //properties.set("key2", prop2)
    //properties.set("key3", prop3)

    val id1: GradoopId = GradoopId.get

    val graphHead: Seq[G] = Seq(EpgmGraphHead.create(Array("Facebook")))
    val vertices: Seq[V] = Seq(EpgmVertex(id1, Array("Person", "Fake Account")))
    val edges: Seq[E] = Seq(EpgmEdge.create(Array("likes") ,id1, id1))

    val graphCollection = config.getGraphCollectionFactory.init(graphHead, vertices, edges)

    spark.stop()
  }
}
