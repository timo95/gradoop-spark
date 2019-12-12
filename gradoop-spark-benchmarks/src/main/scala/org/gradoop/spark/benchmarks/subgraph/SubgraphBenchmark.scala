package org.gradoop.spark.benchmarks.subgraph

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.gradoop.spark.benchmarks.BaseBenchmark
import org.gradoop.spark.expressions.filter.FilterStrings
import org.gradoop.spark.io.impl.csv.{CsvDataSink, CsvDataSource}
import org.rogach.scallop.{ScallopConf, ScallopOption}

object SubgraphBenchmark extends BaseBenchmark {

  val VERTEX_LABEL = "forum"
  val EDGE_LABEL = "hasType"
  val VERIFICATION = false

  class CmdConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val input: ScallopOption[String] = opt[String](required = true,
      descr = "Input path for a csv graph")
    val output: ScallopOption[String] = opt[String](required = true,
      descr = "Output path for a csv graph")
    val verification: ScallopOption[Boolean] = toggle(default = Some(false),
      descrYes = "Verifies the Graph after applying Subgraph")
    val vertexLabel: ScallopOption[String] = opt[String](name = "vl", noshort = true,
      descr = "Label to filter the vertices")
    val edgeLabel: ScallopOption[String] = opt[String](name = "el", noshort = true,
      descr = "Label to filter the edges")
    verify()
  }

  def main(args: Array[String]): Unit = {
    val cmdConf = new CmdConf(args)

    implicit val session: SparkSession = SparkSession.builder
      .appName("Subgraph Benchmark")//.master("local[1]")
      .getOrCreate()
    val config = gveConfig

    val source = CsvDataSource(cmdConf.input(), config)
    var graph = source.readLogicalGraph

    val vertexFilterString: String = if(cmdConf.vertexLabel.isDefined) {
      FilterStrings.hasLabel(cmdConf.vertexLabel())
    } else {
      FilterStrings.any
    }
    val edgeFilterString: String = if(cmdConf.edgeLabel.isDefined) {
      FilterStrings.hasLabel(cmdConf.edgeLabel())
    } else {
      FilterStrings.any
    }
    graph = graph.subgraph(vertexFilterString, edgeFilterString)

    if(cmdConf.verification()) graph = graph.verify

    val sink = CsvDataSink(cmdConf.output(), config)
    sink.write(graph, SaveMode.Overwrite)
  }
}
