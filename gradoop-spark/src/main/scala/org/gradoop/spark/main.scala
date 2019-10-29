package org.gradoop.spark

import org.apache.spark.sql.SparkSession
import org.gradoop.spark.functions.filter.HasLabel
import org.gradoop.spark.util.{EpgmApp, SparkAsciiGraphLoader}

object main extends EpgmApp {
  def main(args: Array[String]): Unit = {

    implicit val session: SparkSession = SparkSession.builder
      .appName("Simple Application")
      .master("local[4]")
      .getOrCreate()

    val config = getGveConfig

    val loader = SparkAsciiGraphLoader.fromString(config, getGraphGDLString)

    var graph = loader.getLogicalGraph

    println("Vertices: " + graph.getVertices.count())

    graph = graph.subgraph(new HasLabel("Person"), e => true)

    println("Vertices Subgraph: " + graph.getVertices.count())

    session.stop()
  }
}
