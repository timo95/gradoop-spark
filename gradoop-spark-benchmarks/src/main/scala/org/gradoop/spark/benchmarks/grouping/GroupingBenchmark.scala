package org.gradoop.spark.benchmarks.grouping

import org.apache.spark.sql.{Column, SparkSession}
import org.gradoop.spark.benchmarks.IoBenchmark
import org.gradoop.spark.expressions.AggregationExpressions
import org.gradoop.spark.functions.{LabelKeyFunction, PropertyKeyFunction}
import org.gradoop.spark.model.impl.operators.grouping.GroupingBuilder
import org.gradoop.spark.model.impl.types.LayoutType
import org.rogach.scallop.ScallopOption

class GroupingBenchmark extends IoBenchmark {

  val COUNT = "count"
  val MIN = "min"
  val MAX = "max"
  val SUM = "sum"

  class CmdConf(arguments: Seq[String]) extends IoConf(arguments) {
    val layout: ScallopOption[String] = opt[String](default = Some("gve"),
      descr = "Graph Layout (gve, tfl)")
    val vertexGroupLabel: ScallopOption[Boolean] = toggle(default = Some(false), name = "gvl", noshort = true,
      descrYes = "Group by vertex label")
    val edgeGroupLabel: ScallopOption[Boolean] = toggle(default = Some(false), name = "gel", noshort = true,
      descrYes = "Group by edge label")
    val vertexGroupProperties: ScallopOption[List[String]] = trailArg[List[String]](default = Some(List.empty),
      name = "gvp", required = false, descr = "Group by vertex property values")
    val edgeGroupProperties: ScallopOption[List[String]] = trailArg[List[String]](default = Some(List.empty),
      name = "gep", required = false, descr = "Group by edge property values")
    val vertexAggregation: ScallopOption[List[String]] = trailArg[List[String]](default = Some(List.empty),
      name = "ga", required = false, descr = "Vertex Aggregation functions (count, min, max, sum)", validate = validateAgg)
    val edgeAggregation: ScallopOption[List[String]] = trailArg[List[String]](default = Some(List.empty),
      name = "ga", required = false, descr = "Edge Aggregation functions (count, min, max, sum)", validate = validateAgg)

    private def validateAgg(strings: List[String]): Boolean = {
      val withArgs = strings.filter(_ != COUNT) // remove functions without arguments
      if(withArgs.length % 2 == 1) false // every remaining function has 1 argument
      else { // key agg functions and property keys have to alternate
        withArgs.zipWithIndex.filter(_._2 % 2 == 0).map(_._1).forall(Set(MIN, MAX, SUM).contains)
      }
    }

    verify()
  }

  def main(args: Array[String]): Unit = {
    implicit val session: SparkSession = SparkSession.builder
      .appName("Grouping Benchmark")//.master("local[1]")
      .getOrCreate()

    val cmdConf = new CmdConf(args)
    cmdConf.layout() match {
      case "gve" => runGveCsv(cmdConf, run[LGve](_, cmdConf))
      case "tfl" => runTflIndexed(cmdConf, run[LTfl](_, cmdConf))
      case layout: Any => throw new IllegalArgumentException("Layout '%s' is not supported.".format(layout))
    }
  }

  private def run[L <: LayoutType[L]](graph: L#LG, cmdConf: CmdConf): L#LG = {
    val groupingBuilder = new GroupingBuilder

    // Grouping keys
    groupingBuilder.vertexGroupingKeys = cmdConf.vertexGroupProperties().map(PropertyKeyFunction.apply)
    groupingBuilder.edgeGroupingKeys = cmdConf.edgeGroupProperties().map(PropertyKeyFunction.apply)
    if(cmdConf.vertexGroupLabel()) groupingBuilder.vertexGroupingKeys =
      groupingBuilder.vertexGroupingKeys :+ new LabelKeyFunction
    if(cmdConf.edgeGroupLabel()) groupingBuilder.edgeGroupingKeys =
      groupingBuilder.edgeGroupingKeys :+ new LabelKeyFunction

    // Aggregation functions
    groupingBuilder.vertexAggFunctions = parseAggFuncs(cmdConf.vertexAggregation())
    groupingBuilder.edgeAggFunctions = parseAggFuncs(cmdConf.edgeAggregation())

    // Run
    graph.groupBy(groupingBuilder)
  }

  private def parseAggFuncs(strings: List[String]): Seq[Column] = {
    val it = strings.iterator
    var agg = Seq.empty[Column]
    while(it.hasNext) {
      it.next match {
        case COUNT => agg = agg :+ AggregationExpressions.count
        case MIN => agg = agg :+ AggregationExpressions.minProp(it.next)
        case MAX => agg = agg :+ AggregationExpressions.maxProp(it.next)
        case SUM => agg = agg :+ AggregationExpressions.sumProp(it.next)
        case any: String => throw new IllegalArgumentException("Aggregate function '%s' is not supported".format(any))
      }
    }
    agg
  }
}
