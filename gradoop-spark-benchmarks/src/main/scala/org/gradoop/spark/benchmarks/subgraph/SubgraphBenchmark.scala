package org.gradoop.spark.benchmarks.subgraph

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.gradoop.spark.benchmarks.BaseBenchmark
import org.gradoop.spark.io.impl.csv.epgm.{EpgmCsvDataSink, EpgmCsvDataSource}

object SubgraphBenchmark extends BaseBenchmark {

  val VERTEX_LABEL = "forum"
  val EDGE_LABEL = "hasType"
  val VERIFICATION = false

  def main(args: Array[String]): Unit = {
    // TODO Cmd parsing
    val inputCsv = args(0)
    val outputCsv = args(1)

    implicit val session: SparkSession = SparkSession.builder
      .appName("Subgraph Benchmark").master("local[1]")
      .getOrCreate()
    val config = gveConfig

    val source = EpgmCsvDataSource(inputCsv, config)
    var graph = source.readLogicalGraph

    graph = graph.subgraph(v => v.labels.equals(VERTEX_LABEL), e => e.labels.equals(EDGE_LABEL))
    if(VERIFICATION) graph = graph.verify

    val sink = EpgmCsvDataSink(outputCsv, config)
    sink.write(graph, SaveMode.Overwrite)
  }
}
