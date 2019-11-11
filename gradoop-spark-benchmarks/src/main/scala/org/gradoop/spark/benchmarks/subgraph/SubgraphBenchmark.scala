package org.gradoop.spark.benchmarks.subgraph

import org.gradoop.spark.io.impl.csv.epgm.{EpgmCsvDataSink, EpgmCsvDataSource}
import org.gradoop.spark.util.EpgmApp

object SubgraphBenchmark extends EpgmApp {

  val VERTEX_LABEL = ""
  val EDGE_LABEL = ""
  val VERIFICATION = true

  def main(args: Array[String]): Unit = {
    // TODO Cmd parsing
    val inputCsv = args(0)
    val outputCsv = args(1)

    val config = gveConfig

    val source = EpgmCsvDataSource(inputCsv, config)

    var graph = source.readLogicalGraph

    graph = graph.subgraph(v => v.labels.equals(VERTEX_LABEL), e => e.labels.equals(EDGE_LABEL))

    if(VERIFICATION) graph = graph.verify

    val sink = EpgmCsvDataSink(outputCsv, config)

    sink.write(graph)
  }
}
