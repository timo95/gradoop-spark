package org.gradoop.spark

import org.apache.spark.sql.{Dataset, Encoder, Encoders, SQLContext, SparkSession}
import org.gradoop.common.model.impl.id.GradoopId
import org.gradoop.common.model.impl.pojo._
import org.gradoop.common.model.impl.properties.{Properties, PropertyValue}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.impl.epgm.layouts.EpgmGveLayout

object main {
  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder
      .appName("Simple Application")
      .master("local[4]")
      .getOrCreate()


    implicit var gradoopIdEncoder: Encoder[EPGMGraphHead] = Encoders.kryo[EPGMGraphHead]
    implicit var vertexEncoder: Encoder[EPGMVertex] = Encoders.kryo[EPGMVertex]
    implicit var edgeEncoder: Encoder[EPGMEdge] = Encoders.kryo[EPGMEdge]

    var prop1 = PropertyValue.create("value1")
    var prop2 = PropertyValue.create(3)
    var prop3 = PropertyValue.create(12.6)

    var properties = new Properties()
    properties.set("key1", prop1)
    properties.set("key2", prop2)
    properties.set("key3", prop3)

    val id1: GradoopId = new GradoopId()

    var gh: Seq[EPGMGraphHead] = Seq(new EPGMGraphHeadFactory().createGraphHead("Graph"))
    val graphHeads: Dataset[EPGMGraphHead] = spark.createDataset(gh)

    var v: Seq[EPGMVertex] = Seq(new EPGMVertexFactory().initVertex(id1, "Person"))
    val vertices: Dataset[EPGMVertex] = spark.createDataset(v)

    var e: Seq[EPGMEdge] = Seq(new EPGMEdgeFactory().createEdge("is", id1, id1))
    val edges: Dataset[EPGMEdge] = spark.createDataset(e)

    val graph = new EpgmGveLayout(graphHeads, vertices, edges)

    val config = new GradoopSparkConfig()

    spark.stop()
  }
}