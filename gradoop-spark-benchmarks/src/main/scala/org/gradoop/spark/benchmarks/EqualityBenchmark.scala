package org.gradoop.spark.benchmarks

import org.apache.spark.sql.SparkSession
import org.gradoop.spark.util.SparkAsciiGraphLoader

object EqualityBenchmark extends BaseBenchmark {

  val graphString = "expected1:g1 {a:\"graph1\",b:2.75d} [" +
    "(v2:B {a:1234L,b:true,c:0.123d})-[e3:b {a:2718L}]->(v3:B {a:5678L,b:false,c:4.123d})]" +
    "expected2:g2 {a:\"graph2\",b:4} [" +
    "(v0:A {a:\"foo\",b:42,c:13.37f,d:NULL})-[e0:a {a:1234,b:13.37f}]->(v1:A {a:\"bar\",b:23,c:19.84f})" +
    "(v1)-[e1:a {a:5678,b:23.42f}]->(v0)" +
    "(v1)-[e2:b {a:3141L}]->(v2:B {a:1234L,b:true,c:0.123d})" +
    "(v2)-[e3]->(v3)" +
    "(v4:B {a:2342L,c:19.84d})-[e4:a {b:19.84f}]->(v0)" +
    "(v4)-[e5:b]->(v0)]"

  def main(args: Array[String]): Unit = {
    implicit val session: SparkSession = SparkSession.builder
      .appName("Equality Benchmark")//.master("local[1]")
      .getOrCreate()
    val config = gveConfig

    val collection1 = SparkAsciiGraphLoader.fromString(config, graphString).getGraphCollection
    val collection2 = SparkAsciiGraphLoader.fromString(config, graphString).getGraphCollection

    println(collection1.equalsByGraphData(collection2))
  }
}
