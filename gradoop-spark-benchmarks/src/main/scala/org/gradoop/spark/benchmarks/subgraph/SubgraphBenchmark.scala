package org.gradoop.spark.benchmarks.subgraph

import org.apache.spark.sql.{Column, SparkSession}
import org.gradoop.spark.benchmarks.IoBenchmark
import org.gradoop.spark.expressions.FilterExpressions
import org.gradoop.spark.model.impl.types.LayoutType
import org.rogach.scallop.ScallopOption

object SubgraphBenchmark extends IoBenchmark {

  class CmdConf(arguments: Seq[String]) extends IoConf(arguments) {
    val title: ScallopOption[String] = opt[String](descr = "Spark job title")
    val layout: ScallopOption[String] = opt[String](default = Some("gve"),
      descr = "Graph Layout (gve, tfl)")
    val removeDanglingEdges: ScallopOption[Boolean] = toggle(default = Some(false),
      descrYes = "Removes dangling edges after applying Subgraph")
    val vertexLabel: ScallopOption[String] = opt[String](descr = "Label to filter the vertices")
    val edgeLabel: ScallopOption[String] = opt[String](descr = "Label to filter the edges")
    verify()
  }

  def main(args: Array[String]): Unit = {
    val cmdConf = new CmdConf(args)

    implicit val session: SparkSession = SparkSession.builder
      .appName(cmdConf.title.getOrElse("Subgraph Benchmark - Layout: %s".format(cmdConf.layout())))//.master("local[1]")
      .getOrCreate()

    cmdConf.layout() match {
      case "gve" => runGveCsv(cmdConf, run[LGve](_, cmdConf))
      case "tfl" => runTflIndexed(cmdConf, run[LTfl](_, cmdConf))
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

    if(cmdConf.removeDanglingEdges()) graph.subgraph(vertexFilterString, edgeFilterString).removeDanglingEdges
    else graph.subgraph(vertexFilterString, edgeFilterString)
  }
}
