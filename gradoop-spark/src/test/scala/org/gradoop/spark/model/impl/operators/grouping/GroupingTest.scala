package org.gradoop.spark.model.impl.operators.grouping

class GroupingTest extends GroupingBehaviors {

  describe("GveGrouping") {
    it should behave like groupingOnlyProperty((graph, builder) => graph.groupBy(builder))
    it should behave like groupingWithLabel((graph, builder) => graph.groupBy(builder))
    it should behave like groupingAggFunctions((graph, builder) => graph.groupBy(builder))
  }

  describe("TflGrouping") {
    it should behave like groupingOnlyProperty((graph, builder) =>
      graph.asTfl(tflConfig).groupBy(builder).asGve(gveConfig))
    it should behave like groupingWithLabel((graph, builder) =>
      graph.asTfl(tflConfig).groupBy(builder).asGve(gveConfig))
    it should behave like groupingAggFunctions((graph, builder) =>
      graph.asTfl(tflConfig).groupBy(builder).asGve(gveConfig))
  }

  describe("Functions") {
    it("getAlias") {
      import org.apache.spark.sql.functions._

      val col1 = max(col("df")).getField("AS ").alias("abc").isNotNull
        .alias("result")
      assert(Functions.getAlias(col1) == "result")

      val col2 = map_from_entries(col("AS df" + col("adsf")))
      assertThrows[IllegalArgumentException](Functions.getAlias(col2))
    }
  }
}
