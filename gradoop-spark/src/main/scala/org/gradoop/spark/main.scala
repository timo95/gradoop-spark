package org.gradoop.spark

import org.apache.spark.sql.SparkSession
import org.gradoop.spark.functions.filter.HasLabel
import org.gradoop.spark.io.impl.csv.CsvDataSource
import org.gradoop.spark.io.impl.csv.epgm.EpgmCsvParser
import org.gradoop.spark.util.{EpgmApp, SparkAsciiGraphLoader}

object main extends EpgmApp {
  def main(args: Array[String]): Unit = {

    implicit val session: SparkSession = SparkSession.builder
      .appName("Simple Application")
      .master("local[4]")
      .getOrCreate()

    val config = getGveConfig

    //val loader = SparkAsciiGraphLoader.fromString(config, getGraphGDLString)

    //var graph = loader.getLogicalGraph

    val csvDataSource = new CsvDataSource[G, V, E, LG, GC]("/home/timo/Projekte/graphs/ldbc_1", config,
      new EpgmCsvParser(None))

    val graph = csvDataSource.getLogicalGraph

    println("Graphs: " + graph.getGraphHead.count())
    println("Vertices: " + graph.getVertices.count())
    println("Edges: " + graph.getEdges.count())

    //graph = graph.subgraph(new HasLabel("Person"), e => true)

    //println("Vertices Subgraph: " + graph.getVertices.count())

    session.stop()
  }
}
