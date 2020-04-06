package org.gradoop.spark.benchmarks.util

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.gradoop.spark.io.impl.csv.CsvDataSource
import org.gradoop.spark.io.impl.csv.indexed.IndexedCsvDataSink
import org.gradoop.spark.util.EpgmConfigs
import org.rogach.scallop.{ScallopConf, ScallopOption}

object CsvToIndexed extends EpgmConfigs {

  class Conf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val input: ScallopOption[String] = opt[String](required = true,
      descr = "Input path for a csv graph")
    val output: ScallopOption[String] = opt[String](required = true,
      descr = "Output path for a indexed csv graph")
  }

  def main(args: Array[String]): Unit = {
    val conf = new Conf(args)

    implicit val session: SparkSession = SparkSession.builder
      .appName("Csv to Indexed Csv")//.master("local[1]")
      .getOrCreate()

    val source = CsvDataSource(conf.input(), gveConfig)
    val graph = source.readLogicalGraph
    val sink = IndexedCsvDataSink(conf.output(), tflConfig)
    sink.write(graph.asTfl(tflConfig), SaveMode.Overwrite)
  }
}
