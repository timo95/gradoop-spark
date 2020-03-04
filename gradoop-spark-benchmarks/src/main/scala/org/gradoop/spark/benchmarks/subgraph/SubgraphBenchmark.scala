package org.gradoop.spark.benchmarks.subgraph

import org.apache.spark.sql.Column
import org.gradoop.spark.benchmarks.IoBenchmark
import org.gradoop.spark.benchmarks.IoBenchmark.IoConf
import org.gradoop.spark.benchmarks.subgraph.SubgraphBenchmark.SubgraphConf
import org.gradoop.spark.expressions.FilterExpressions
import org.gradoop.spark.model.impl.types.LayoutType
import org.rogach.scallop.ScallopOption

object SubgraphBenchmark extends IoBenchmark[SubgraphConf] {

  override def getConf(args: Array[String]): SubgraphConf = new SubgraphConf(args)

  class SubgraphConf(arguments: Seq[String]) extends IoConf(arguments) {
    val removeDanglingEdges: ScallopOption[Boolean] = toggle(default = Some(false),
      descrYes = "Removes dangling edges after applying Subgraph")
    val vertexLabel: ScallopOption[String] = opt[String](descr = "Label to filter the vertices")
    val edgeLabel: ScallopOption[String] = opt[String](descr = "Label to filter the edges")
    verify()
  }

  override def run[L <: LayoutType[L]](conf: SubgraphConf, graph: L#LG): L#LG = {
    val vertexFilterString = conf.vertexLabel.toOption match {
      case Some(label) => FilterExpressions.hasLabel(label)
      case None => FilterExpressions.any
    }
    val edgeFilterString: Column = conf.edgeLabel.toOption match {
      case Some(label) => FilterExpressions.hasLabel(label)
      case None => FilterExpressions.any
    }

    if(conf.removeDanglingEdges()) graph.subgraph(vertexFilterString, edgeFilterString).removeDanglingEdges
    else graph.subgraph(vertexFilterString, edgeFilterString)
  }
}
