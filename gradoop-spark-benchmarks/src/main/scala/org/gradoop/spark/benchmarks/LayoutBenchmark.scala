package org.gradoop.spark.benchmarks

import org.apache.spark.sql.SparkSession
import org.gradoop.spark.benchmarks.LayoutBenchmark.LayoutConf
import org.gradoop.spark.util.EpgmConfigs
import org.rogach.scallop.{ScallopConf, ScallopOption}

trait LayoutBenchmark[A <: LayoutConf] extends EpgmConfigs {

  def runGve(conf: A)(implicit sparkSession: SparkSession): Unit

  def runTfl(conf: A)(implicit sparkSession: SparkSession): Unit

  def getConf(args: Array[String]): A

  def main(args: Array[String]): Unit = {
    val conf: A = getConf(args)

    implicit val session: SparkSession = SparkSession.builder
      .appName(conf.name.getOrElse("Benchmark - Layout: %s".format(conf.layout())))//.master("local[1]")
      .getOrCreate()

    conf.layout().toLowerCase match {
      case "gve" => runGve(conf)
      case "tfl" => runTfl(conf)
      case layout: Any => throw new IllegalArgumentException("Layout '%s' is not supported.".format(layout))
    }
  }
}

object LayoutBenchmark {
  class LayoutConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val layout: ScallopOption[String] = opt[String](default = Some("gve"),
      descr = "Graph Layout (gve, tfl)")
    val name: ScallopOption[String] = opt[String](descr = "Spark job name")
  }
}
