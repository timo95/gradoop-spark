package org.gradoop.spark.benchmarks.grouping


import org.gradoop.spark.benchmarks.IoBenchmark
import org.gradoop.spark.benchmarks.IoBenchmark.IoConf
import org.gradoop.spark.benchmarks.grouping.GroupingBenchmark.{MAX, MIN, SUM}
import org.gradoop.spark.functions.aggregation._
import org.gradoop.spark.functions.{LabelKeyFunction, PropertyKeyFunction}
import org.gradoop.spark.model.impl.operators.grouping.GroupingBuilder
import org.gradoop.spark.model.impl.types.LayoutType
import org.rogach.scallop.ScallopOption

object GroupingBenchmark extends IoBenchmark[GroupingConf] {

  val COUNT = "count"
  val MIN = "min"
  val MAX = "max"
  val SUM = "sum"

  override def getConf(args: Array[String]): GroupingConf = new GroupingConf(args)

  override def run[L <: LayoutType[L]](conf: GroupingConf, graph: L#LG): L#LG = {
    val groupingBuilder = new GroupingBuilder

    // Grouping keys
    groupingBuilder.vertexGroupingKeys = conf.vertexGroupProperties().map(PropertyKeyFunction.apply)
    groupingBuilder.edgeGroupingKeys = conf.edgeGroupProperties().map(PropertyKeyFunction.apply)
    if(conf.vertexGroupLabel()) groupingBuilder.vertexGroupingKeys =
      groupingBuilder.vertexGroupingKeys :+ new LabelKeyFunction
    if(conf.edgeGroupLabel()) groupingBuilder.edgeGroupingKeys =
      groupingBuilder.edgeGroupingKeys :+ new LabelKeyFunction

    // Aggregation functions
    groupingBuilder.vertexAggFunctions = parseAggFuncs(conf.vertexAggregation())
    groupingBuilder.edgeAggFunctions = parseAggFuncs(conf.edgeAggregation())

    println(groupingBuilder)

    // Run
    graph.groupBy(groupingBuilder)
  }

  /** Parse list of strings as aggregation functions.
   *  The string list has to be of the form "function1 [, function1 arguments], function2, ..."
   *  with the aggregation function names and arguments corresponding to the following list:
   *
   *  count: -
   *  min: Property key
   *  max: Property key
   *  sum: Property key
   */
  private def parseAggFuncs(strings: List[String]): Seq[AggregationFunction] = {
    val it = strings.iterator
    var agg = Seq.empty[AggregationFunction]
    while(it.hasNext) {
      it.next match {
        case GroupingBenchmark.COUNT => agg = agg :+ Count()
        case GroupingBenchmark.MIN => agg = agg :+ Min(it.next)
        case GroupingBenchmark.MAX => agg = agg :+ Max(it.next)
        case GroupingBenchmark.SUM => agg = agg :+ Sum(it.next)
        case any: String => throw new IllegalArgumentException("Aggregate function '%s' is not supported".format(any))
      }
    }
    agg
  }
}

class GroupingConf(arguments: Seq[String]) extends IoConf(arguments) {
  val vertexGroupLabel: ScallopOption[Boolean] = toggle(default = Some(false),
    name = "gvl", noshort = true, descrYes = "Group by vertex label")
  val edgeGroupLabel: ScallopOption[Boolean] = toggle(default = Some(false),
    name = "gel", noshort = true, descrYes = "Group by edge label")

  val vertexGroupProperties: ScallopOption[List[String]] = opt[List[String]](default = Some(List.empty),
    name = "gvp", noshort = true, descr = "Group by vertex property values")
  val edgeGroupProperties: ScallopOption[List[String]] = opt[List[String]](default = Some(List.empty),
    name = "gep", noshort = true, descr = "Group by edge property values")

  val vertexAggregation: ScallopOption[List[String]] = opt[List[String]](default = Some(List.empty),
    name = "gva", noshort = true, descr = "Vertex Aggregation functions (count, min, max, sum)", validate = validateAgg)
  val edgeAggregation: ScallopOption[List[String]] = opt[List[String]](default = Some(List.empty),
    name = "gea", noshort = true, descr = "Edge Aggregation functions (count, min, max, sum)", validate = validateAgg)

  private def validateAgg(strings: List[String]): Boolean = {
    val withArgs = strings.filter(_ != GroupingBenchmark.COUNT) // remove functions without arguments
    if(withArgs.length % 2 == 1) false // every remaining function has 1 argument
    else { // key agg functions and property keys should alternate
      withArgs.zipWithIndex.filter(_._2 % 2 == 0).map(_._1).forall(Set(MIN, MAX, SUM).contains)
    }
  }

  verify()
}
