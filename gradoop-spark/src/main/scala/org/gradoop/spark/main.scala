package org.gradoop.spark

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.gradoop.spark.functions.filter.HasLabel
import org.gradoop.spark.io.impl.csv.CsvDataSource
import org.gradoop.spark.io.impl.csv.epgm.{EpgmCsvDataSink, EpgmCsvDataSource}
import org.gradoop.spark.model.impl.elements.EpgmGraphHead
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

    val csvDataSource = EpgmCsvDataSource("/home/timo/Projekte/graphs/ldbc_1", config)
    //val csvDataSink = EpgmCsvDataSink("/home/timo/Projekte/graphs/ldbc_1_out", config)

    val graph = csvDataSource.getLogicalGraph

    graph.getGraphHead.foreach(g => println(g.getLabels.length))
    graph.getGraphHead.foreach(g => println(g.getLabels))
    graph.getGraphHead.foreach(g => println(g.getLabels(0)))

    import config.implicits._
    import session.implicits._
    graph.getGraphHead.map(g => g.getId).map(EpgmGraphHead.apply).map(g => g.getLabels).foreach(l => l.foreach(println))

    //println("Graphs: " + graph.getGraphHead.count())
    //println("Vertices: " + graph.getVertices.count())
    //println("Edges: " + graph.getEdges.count())

    //csvDataSink.write(graph, SaveMode.Overwrite)

    //graph = graph.subgraph(new HasLabel("Person"), e => true)

    //println("Vertices Subgraph: " + graph.getVertices.count())

    session.stop()
  }
}
