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
}
