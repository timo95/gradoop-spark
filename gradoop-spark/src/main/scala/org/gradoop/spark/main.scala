package org.gradoop.spark

import org.apache.spark.sql.{Dataset, Encoder, Encoders, SparkSession}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.elements.{EdgeFactory, VertexFactory}
import org.gradoop.spark.model.api.graph.LogicalGraphFactory
import org.gradoop.spark.model.api.layouts.LogicalGraphLayoutFactory
import org.gradoop.spark.model.impl.elements.{EpgmEdge, EpgmGraphHead, EpgmVertex}
import org.gradoop.spark.model.impl.graph.{EpgmGraphCollection, EpgmLogicalGraph}
import org.gradoop.spark.model.impl.types.EpgmGraphModel
import org.gradoop.spark.util.{EpgmGradoopSparkConfig, GradoopId}

object main extends EpgmGraphModel {
  def main(args: Array[String]): Unit = {

    implicit val spark: SparkSession = SparkSession.builder
      .appName("Simple Application")
      .master("local[4]")
      .getOrCreate()

    implicit var gradoopIdEncoder: Encoder[G] = Encoders.kryo[G]
    implicit var vertexEncoder: Encoder[V] = Encoders.kryo[V]
    implicit var edgeEncoder: Encoder[E] = Encoders.kryo[E]

    //var prop1 = PropertyValue.create("value1")
    //var prop2 = PropertyValue.create(3)
    //var prop3 = PropertyValue.create(12.6)

    //var properties = new Properties()
    //properties.set("key1", prop1)
    //properties.set("key2", prop2)
    //properties.set("key3", prop3)

    val id1: GradoopId = GradoopId.get

    var gh: Seq[G] = Seq(EpgmGraphHead.create(Array("Graph")))
    val graphHeads: Dataset[G] = spark.createDataset(gh)

    var v: Seq[V] = Seq(EpgmVertex(id1, Array("Person")))
    val vertices: Dataset[V] = spark.createDataset(v)

    var e: Seq[E] = Seq(EpgmEdge.create(Array("likes") ,id1, id1))
    val edges: Dataset[E] = spark.createDataset(e)

    edges.printSchema()
    graphHeads.printSchema()
    edges.printSchema()

    var fac: VertexFactory[V] = EpgmVertex



    implicit val config: GradoopSparkConfig[G, V, E, EpgmLogicalGraph, EpgmGraphCollection] = new EpgmGradoopSparkConfig()
    //implicit def ftolf(f: LogicalGraphFactory[G, V, E, EpgmLogicalGraph, EpgmGraphCollection]): LogicalGraphLayoutFactory[G, V, E, EpgmLogicalGraph, EpgmGraphCollection] = f.getLayoutFactory

    config.getLogicalGraphFactory.createEmptyGraph

    spark.stop()
  }
}
