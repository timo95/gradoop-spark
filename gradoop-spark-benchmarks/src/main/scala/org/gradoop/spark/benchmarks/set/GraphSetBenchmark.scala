package org.gradoop.spark.benchmarks.set

import org.gradoop.spark.benchmarks.BiIoBenchmark
import org.gradoop.spark.benchmarks.BiIoBenchmark.BiIoConf
import org.gradoop.spark.model.impl.types.LayoutType
import org.rogach.scallop.ScallopOption

object GraphSetBenchmark extends BiIoBenchmark[GraphSetConf] {

  override def getConf(args: Array[String]): GraphSetConf = new GraphSetConf(args)

  override def run[L <: LayoutType[L]](conf: GraphSetConf, left: L#LG, right: L#LG): L#LG = {
    conf.setOperator().toLowerCase match {
      case "combination" | "c" => left.combine(right)
      case "exclusion" | "e" => left.exclude(right)
      case "overlap" | "o" => left.overlap(right)
      case op: Any => throw new  IllegalArgumentException(s"Operator $op is not supported.")
    }
  }
}

class GraphSetConf(arguments: Seq[String]) extends BiIoConf(arguments) {
  val setOperator: ScallopOption[String] = opt[String](required = true,
    descr = "Set operator to run (combination, exclusion, overlap)")
  verify()
}
