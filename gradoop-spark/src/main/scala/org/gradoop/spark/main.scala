package org.gradoop.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
import org.gradoop.common.properties.PropertyValue
import org.gradoop.spark.io.impl.csv.CsvDataSource
import org.gradoop.spark.io.impl.csv.epgm.{EpgmCsvDataSink, EpgmCsvDataSource}
import org.gradoop.spark.model.impl.elements.{EpgmGraphHead, PV}
import org.gradoop.spark.model.impl.operators.transform.GraphHeadToRow
import org.gradoop.spark.util.{EpgmApp, SparkAsciiGraphLoader}

object main extends EpgmApp {
  def main(args: Array[String]): Unit = {

    val config = gveConfig

    //val loader = SparkAsciiGraphLoader.fromString(config, getGraphGDLString)

    //var graph = loader.getLogicalGraph

    val csvDataSource = EpgmCsvDataSource("/home/timo/Projekte/graphs/ldbc_1", config)
    //val csvDataSink = EpgmCsvDataSink("/home/timo/Projekte/graphs/ldbc_1_out", config)

    val graph = csvDataSource.readLogicalGraph

    graph.graphHead.foreach(g => println(g.labels.length))
    graph.graphHead.foreach(g => println(g.labels))
    //graph.getGraphHead.foreach(g => println(g.getLabels(0)))

    import config.implicits._

    import session.implicits._

    graph.graphHead.show(false)
    graph.vertices.show(false)
    graph.edges.show(false)

    //csvDataSink.write(graph, SaveMode.Overwrite)

    //graph = graph.subgraph(new HasLabel("Person"), e => true)

    //println("Vertices Subgraph: " + graph.getVertices.count())

    session.stop()
  }
}
