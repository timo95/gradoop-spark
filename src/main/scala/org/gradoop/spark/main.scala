package org.gradoop.spark

import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.gradoop.common.model.impl.id.GradoopId
import org.gradoop.common.model.impl.pojo._
import org.gradoop.common.model.impl.properties.{Properties, PropertyValue}
import org.gradoop.spark.model.impl.layouts.GVELayout

object main {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .appName("Simple Application")
      .master("local[4]")
      .getOrCreate()

    val sqlContext = spark.sqlContext
    //import sqlContext.implicits._

    val data = Seq(("Person", 13),("Tier", 3),("Person", 42))
    val dataRDD = spark.sparkContext.parallelize(data)



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

    var id1: GradoopId = new GradoopId()
    var id2: GradoopId = new GradoopId()

    var gh: Seq[EPGMGraphHead] = Seq(new EPGMGraphHeadFactory().createGraphHead("Graph"))
    val graphHeads: Dataset[EPGMGraphHead] = sqlContext.createDataset(gh)

    var v: Seq[EPGMVertex] = Seq(new EPGMVertexFactory().initVertex(id1, "Person"))
    val vertices: Dataset[EPGMVertex] = sqlContext.createDataset(v)

    var e: Seq[EPGMEdge] = Seq(new EPGMEdgeFactory().createEdge("is", id1, id1))
    val edges: Dataset[EPGMEdge] = sqlContext.createDataset(e)

    val graph = new GVELayout(graphHeads, vertices, edges)

    spark.stop()
  }
}