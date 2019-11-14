package org.gradoop.spark

import org.apache.spark.SparkConf
import org.apache.spark.sql.{Encoder, Encoders, SaveMode, SparkSession}
import org.gradoop.common.properties.PropertyValue
import org.gradoop.spark.io.impl.csv.{CsvDataSink, CsvDataSource}
import org.gradoop.spark.model.impl.elements.{EpgmGraphHead, PV}
import org.gradoop.spark.util.{EpgmApp, SparkAsciiGraphLoader}

object main extends EpgmApp {
  def main(args: Array[String]): Unit = {

    val config = gveConfig
    import config.implicits._
    import session.implicits._

    val csvDataSource = CsvDataSource("/home/timo/Projekte/graphs/ldbc_1", config)
    val csvDataSink = CsvDataSink("/home/timo/Projekte/graphs/ldbc_1_out", config)

    //val graph = csvDataSource.readLogicalGraph

    //graph.graphHead.foreach(g => println(g.labels.length))
    //graph.graphHead.foreach(g => println(g.labels))
    //graph.getGraphHead.foreach(g => println(g.getLabels(0)))

    //graph.graphHead.foreach(g => g.properties.foreach(println))
    //graph.graphHead.display

    //csvDataSink.write(graph.factory.init(graph.graphHead, session.emptyDataset[V], session.emptyDataset[E]), SaveMode.Overwrite)

    //graph = graph.subgraph(new HasLabel("Person"), e => true)

    //println("Vertices Subgraph: " + graph.getVertices.count())

    session.stop()
  }
}
