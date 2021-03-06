package org.gradoop.spark.benchmarks

import org.apache.spark.sql.{SaveMode, SparkSession}
import org.gradoop.spark.benchmarks.IoBenchmark.IoConf
import org.gradoop.spark.benchmarks.LayoutBenchmark.LayoutConf
import org.gradoop.spark.io.impl.csv.indexed.{IndexedCsvDataSink, IndexedCsvDataSource}
import org.gradoop.spark.io.impl.csv.{CsvDataSink, CsvDataSource}
import org.gradoop.spark.model.impl.types.LayoutType
import org.rogach.scallop.ScallopOption

/**
 * Benchmark that reads a (indexed) csv graph, runs some operation and writes the result as (indexed) csv.
 */
trait IoBenchmark[A <: IoConf] extends LayoutBenchmark[A] {

  /** Run unary graph operator on given graph with layout L.
   *
   * @param conf command line options
   * @param graph input graph
   * @tparam L graph layout
   * @return result graph
   */
  def run[L <: LayoutType[L]](conf: A, graph: L#LG): L#LG

  /** Reads csv graph as gve, runs function and writes result as csv.
   *
   * @param conf command line config with input and output paths
   * @param sparkSession spark session
   */
  override def runGve(conf: A)(implicit sparkSession: SparkSession): Unit = {
    val source = CsvDataSource(conf.input(), gveConfig)
    val graph = source.readLogicalGraph
    val sink = CsvDataSink(conf.output(), gveConfig)
    sink.write(run(conf, graph), SaveMode.Overwrite)
  }

  /** Reads indexed csv graph as tfl, runs function and writes result as indexed csv.
   *
   * @param conf command line config with input and output paths
   * @param sparkSession spark session
   */
  override def runTfl(conf: A)(implicit sparkSession: SparkSession): Unit = {
    val source = IndexedCsvDataSource(conf.input(), tflConfig)
    val graph = source.readLogicalGraph
    val sink = IndexedCsvDataSink(conf.output(), tflConfig)
    sink.write(run(conf, graph), SaveMode.Overwrite)
  }
}

object IoBenchmark {
  abstract class IoConf(arguments: Seq[String]) extends LayoutConf(arguments) {
    val input: ScallopOption[String] = opt[String](required = true,
      descr = "Input path for a csv graph")
    val output: ScallopOption[String] = opt[String](required = true,
      descr = "Output path for a csv graph")
  }
}
