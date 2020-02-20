package org.gradoop.spark.benchmarks.subgraph

import org.apache.spark.sql.{Column, SparkSession}
import org.gradoop.spark.benchmarks.IoBenchmark
import org.gradoop.spark.expressions.FilterExpressions
import org.gradoop.spark.model.impl.types.LayoutType
import org.rogach.scallop.ScallopOption

object SubgraphBenchmark extends IoBenchmark {

  val VERTEX_LABEL = "forum"
  val EDGE_LABEL = "hasType"
  val VERIFICATION = false

  class CmdConf(arguments: Seq[String]) extends IoConf(arguments) {
    val layout: ScallopOption[String] = opt[String](default = Some("gve"),
      descr = "Graph Layout (gve, tfl)")
    val verifySubgraph: ScallopOption[Boolean] = toggle(default = Some(false),
      descrYes = "Verifies the Graph after applying Subgraph")
    val vertexLabel: ScallopOption[String] = opt[String](name = "vl", noshort = true,
      descr = "Label to filter the vertices")
    val edgeLabel: ScallopOption[String] = opt[String](name = "el", noshort = true,
      descr = "Label to filter the edges")
    verify()
  }

  def main(args: Array[String]): Unit = {
    implicit val session: SparkSession = SparkSession.builder
      .appName("Subgraph Benchmark")//.master("local[1]")
      .getOrCreate()

    val cmdConf = new CmdConf(args)
    cmdConf.layout() match {
      case "gve" => runGve(cmdConf, run(_, cmdConf))
      case "tfl" => runTfl(cmdConf, run(_, cmdConf))
      case layout: Any => throw new IllegalArgumentException("Layout '%s' is not supported.".format(layout))
    }
  }

  private def run[L <: LayoutType[L]](graph: L#LG, cmdConf: CmdConf): L#LG = {
    val vertexFilterString = cmdConf.vertexLabel.toOption match {
      case Some(label) => FilterExpressions.hasLabel(label)
      case None => FilterExpressions.any
    }
    val edgeFilterString: Column = cmdConf.edgeLabel.toOption match {
      case Some(label) => FilterExpressions.hasLabel(label)
      case None => FilterExpressions.any
    }

    if(cmdConf.verifySubgraph()) graph.subgraph(vertexFilterString, edgeFilterString).verify
    else graph.subgraph(vertexFilterString, edgeFilterString)
  }
}
