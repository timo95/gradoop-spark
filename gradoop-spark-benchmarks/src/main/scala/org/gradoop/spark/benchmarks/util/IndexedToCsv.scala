package org.gradoop.spark.benchmarks.util

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.gradoop.spark.io.impl.csv.CsvDataSink
import org.gradoop.spark.io.impl.csv.indexed.IndexedCsvDataSource
import org.gradoop.spark.util.EpgmConfigs
import org.rogach.scallop.{ScallopConf, ScallopOption}

object IndexedToCsv extends EpgmConfigs {

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val input: ScallopOption[String] = opt[String](required = true,
      descr = "Input path for a indexed csv graph")
    val output: ScallopOption[String] = opt[String](required = true,
      descr = "Output path for a csv graph")

    verify()
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)

    implicit val session: SparkSession = SparkSession.builder
      .appName("Indexed Csv to Csv: %s to %s".format(conf.input(), conf.output()))//.master("local[1]")
      .getOrCreate()

    val source = IndexedCsvDataSource(conf.input(), tflConfig)
    val graph = source.readLogicalGraph
    val sink = CsvDataSink(conf.output(), gveConfig)
    sink.write(graph.asGve(gveConfig), SaveMode.Overwrite)
  }
}
