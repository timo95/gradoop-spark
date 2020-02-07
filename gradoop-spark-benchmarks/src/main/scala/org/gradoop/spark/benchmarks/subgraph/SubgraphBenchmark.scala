package org.gradoop.spark.benchmarks.subgraph

import org.apache.spark.sql.{Column, SaveMode, SparkSession}
import org.gradoop.spark.benchmarks.BaseBenchmark
import org.gradoop.spark.expressions.FilterExpressions
import org.gradoop.spark.io.impl.csv.{CsvDataSink, CsvDataSource}
import org.gradoop.spark.model.impl.types.LayoutType
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
    val layout: ScallopOption[String] = opt[String](default = Some("gve"),
      descr = "Graph Layout (gve, tfl)")
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
    cmdConf.layout() match {
      case "gve" => runGve(cmdConf)
      case "tfl" => runTfl(cmdConf)
      case layout: Any => throw new IllegalArgumentException("Layout '%s' is not supported.".format(layout))
    }
  }

  private def runGve(cmdConf: CmdConf): Unit = {
    implicit val session: SparkSession = SparkSession.builder
      .appName("GveSubgraph Benchmark")//.master("local[1]")
      .getOrCreate()
    val config = gveConfig

    val source = CsvDataSource(cmdConf.input(), config)
    val graph = source.readLogicalGraph
    val subgraph = run(graph, cmdConf)

    val sink = CsvDataSink(cmdConf.output(), config)
    sink.write(subgraph, SaveMode.Overwrite)
  }

  private def runTfl(cmdConf: CmdConf): Unit = {
    implicit val session: SparkSession = SparkSession.builder
      .appName("TflSubgraph Benchmark")//.master("local[1]")
      .getOrCreate()

    val source = CsvDataSource(cmdConf.input(), gveConfig)
    val graph = source.readLogicalGraph.asTfl(tflConfig)
    val subgraph = run(graph, cmdConf)

    val sink = CsvDataSink(cmdConf.output(), gveConfig)
    sink.write(subgraph.asGve(gveConfig), SaveMode.Overwrite)
  }

  private def run[L <: LayoutType[L]](graph: L#LG, cmdConf: CmdConf): L#LG = {
    val vertexFilterString: Column = if(cmdConf.vertexLabel.isDefined) {
      FilterExpressions.hasLabel(cmdConf.vertexLabel())
    } else {
      FilterExpressions.any
    }
    val edgeFilterString: Column = if(cmdConf.edgeLabel.isDefined) {
      FilterExpressions.hasLabel(cmdConf.edgeLabel())
    } else {
      FilterExpressions.any
    }

    if(cmdConf.verification()) graph.subgraph(vertexFilterString, edgeFilterString).verify
    else graph.subgraph(vertexFilterString, edgeFilterString)
  }
}
