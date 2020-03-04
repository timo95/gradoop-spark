package org.gradoop.spark.benchmarks.set

import org.gradoop.spark.benchmarks.BiIoBenchmark.BiIoConf
import org.gradoop.spark.benchmarks.BiIoCollectionBenchmark
import org.gradoop.spark.benchmarks.set.CollectionSetBenchmark.CollectionSetConf
import org.gradoop.spark.model.impl.types.LayoutType
import org.rogach.scallop.ScallopOption

object CollectionSetBenchmark extends BiIoCollectionBenchmark[CollectionSetConf] {

  class CollectionSetConf(arguments: Seq[String]) extends BiIoConf(arguments) {
    val setOperator: ScallopOption[String] = opt[String](required = true,
      descr = "Set operator to run (union, difference, intersection)")
    verify()
  }

  override def getConf(args: Array[String]): CollectionSetConf = new CollectionSetConf(args)

  override def run[L <: LayoutType[L]](conf: CollectionSetConf, left: L#GC, right: L#GC): L#GC = {
    conf.setOperator().toLowerCase match {
      case "union" | "u" => left.union(right)
      case "difference" | "d" => left.difference(right)
      case "intersection" | "i" => left.intersect(right)
      case op: Any => throw new  IllegalArgumentException(s"Operator $op is not supported.")
    }
  }
}
