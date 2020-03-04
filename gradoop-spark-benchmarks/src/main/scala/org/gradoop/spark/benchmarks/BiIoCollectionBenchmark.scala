package org.gradoop.spark.benchmarks

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.gradoop.spark.benchmarks.BiIoCollectionBenchmark.BiIoCollectionConf
import org.gradoop.spark.benchmarks.LayoutBenchmark.LayoutConf
import org.gradoop.spark.io.impl.csv.indexed.{IndexedCsvDataSink, IndexedCsvDataSource}
import org.gradoop.spark.io.impl.csv.{CsvDataSink, CsvDataSource}
import org.gradoop.spark.model.impl.types.LayoutType
import org.rogach.scallop.ScallopOption

/**
 * Benchmark that reads two (indexed) csv graphs collections,
 * runs some operation and writes the result as (indexed) csv.
 */
trait BiIoCollectionBenchmark[A <: BiIoCollectionConf] extends LayoutBenchmark[A] {

  /** Run binary graph operator on given graphs with layout L.
   *
   * @param conf command line options
   * @param left input graph
   * @param right input graph
   * @tparam L graph layout
   * @return result graph
   */
  def run[L <: LayoutType[L]](conf: A, left: L#GC, right: L#GC): L#GC

  /** Reads csv graph as gve, runs function and writes result as csv.
   *
   * @param conf command line config with input and output paths
   * @param sparkSession spark session
   */
  override def runGve(conf: A)(implicit sparkSession: SparkSession): Unit = {
    val left = CsvDataSource(conf.input1(), gveConfig).readGraphCollection
    val right = CsvDataSource(conf.input2(), gveConfig).readGraphCollection
    val sink = CsvDataSink(conf.output(), gveConfig)
    sink.write(run(conf, left, right), SaveMode.Overwrite)
  }

  /** Reads indexed csv graph as tfl, runs function and writes result as indexed csv.
   *
   * @param conf command line config with input and output paths
   * @param sparkSession spark session
   */
  override def runTfl(conf: A)(implicit sparkSession: SparkSession): Unit = {
    val left = IndexedCsvDataSource(conf.input1(), tflConfig).readGraphCollection
    val right = IndexedCsvDataSource(conf.input2(), tflConfig).readGraphCollection
    val sink = IndexedCsvDataSink(conf.output(), tflConfig)
    sink.write(run(conf, left, right), SaveMode.Overwrite)
  }
}

object BiIoCollectionBenchmark {
  abstract class BiIoCollectionConf(arguments: Seq[String]) extends LayoutConf(arguments) {
    val input1: ScallopOption[String] = opt[String](required = true,
      descr = "Input path for left csv graph collection 1")
    val input2: ScallopOption[String] = opt[String](required = true,
      descr = "Input path for csv graph collection 2")
    val output: ScallopOption[String] = opt[String](required = true,
      descr = "Output path for a csv graph collection")
  }
}
