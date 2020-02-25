package org.gradoop.spark.benchmarks

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.gradoop.spark.io.impl.csv.indexed.{IndexedCsvDataSink, IndexedCsvDataSource}
import org.gradoop.spark.io.impl.csv.{CsvDataSink, CsvDataSource}
import org.gradoop.spark.util.EpgmConfigs
import org.rogach.scallop.{ScallopConf, ScallopOption}

trait IoBenchmark extends EpgmConfigs {

  class IoConf(arguments: Seq[String]) extends ScallopConf(arguments) {
    val input: ScallopOption[String] = opt[String](required = true,
      descr = "Input path for a csv graph")
    val output: ScallopOption[String] = opt[String](required = true,
      descr = "Output path for a csv graph")
    verify()
  }

  /** Reads csv graph as gve, runs function and writes result as csv.
   *
   * @param cmdConf command line config with input and output paths
   * @param run benchmark function
   * @param sparkSession spark session
   */
  protected def runGveCsv(cmdConf: IoConf, run: LGve#LG => LGve#LG)(implicit sparkSession: SparkSession): Unit = {
    val source = CsvDataSource(cmdConf.input(), gveConfig)
    val graph = source.readLogicalGraph
    val sink = CsvDataSink(cmdConf.output(), gveConfig)
    sink.write(run(graph), SaveMode.Overwrite)
  }

  /** Reads indexed csv graph as tfl, runs function and writes result as indexed csv.
   *
   * @param cmdConf command line config with input and output paths
   * @param run benchmark function
   * @param sparkSession spark session
   */
  protected def runTflIndexed(cmdConf: IoConf, run: LTfl#LG => LTfl#LG)(implicit sparkSession: SparkSession): Unit = {
    val source = IndexedCsvDataSource(cmdConf.input(), tflConfig)
    val graph = source.readLogicalGraph
    val sink = IndexedCsvDataSink(cmdConf.output(), tflConfig)
    sink.write(run(graph), SaveMode.Overwrite)
  }
}
