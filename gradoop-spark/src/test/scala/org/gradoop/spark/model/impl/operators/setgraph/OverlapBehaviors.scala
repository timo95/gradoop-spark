package org.gradoop.spark.model.impl.operators.setgraph

import org.gradoop.common.properties.PropertyValue
import org.gradoop.spark.expressions.FilterExpressions
import org.gradoop.spark.util.SparkAsciiGraphLoader
import org.gradoop.spark.{EpgmGradoopSparkTestBase, OperatorTest}
import org.scalatest.FunSpec

trait OverlapBehaviors extends EpgmGradoopSparkTestBase {
  this: FunSpec =>

  def overlap(runOverlap: (LGve#LG, LGve#LG) => LGve#LG) {
    it("Overlapping graphs", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected[(alice)-[akb]->(bob)-[bka]->(alice)]")

      val left = loader.getLogicalGraphByVariable("g0")
      val right = loader.getLogicalGraphByVariable("g2")
      val expected = loader.getLogicalGraphByVariable("expected")

      assert(runOverlap(left, right).equalsByElementData(expected))
      assert(runOverlap(right, left).equalsByElementData(expected))
    }

    it("Overlapping graphs derived", OperatorTest) {
      val loader = SparkAsciiGraphLoader.fromString(gveConfig,
        "g[(a)]" +
          "expected[(a)]")

      val left = loader.getLogicalGraphByVariable("g")
        .vertexInducedSubgraph(FilterExpressions.any)
      val right = loader.getLogicalGraphByVariable("g")
        .vertexInducedSubgraph(FilterExpressions.any)
      val expected = loader.getLogicalGraphByVariable("expected")

      assert(runOverlap(left, right).equalsByElementData(expected))
    }

    it("Non overlapping graphs", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected[]")

      val left = loader.getLogicalGraphByVariable("g0")
      val right = loader.getLogicalGraphByVariable("g1")
      val expected = loader.getLogicalGraphByVariable("expected")

      assert(runOverlap(left, right).equalsByElementData(expected))
      assert(runOverlap(right, left).equalsByElementData(expected))
    }

    it("Non overlapping graphs derived", OperatorTest) {
      val loader = SparkAsciiGraphLoader.fromString(gveConfig,
        "g[(a {x: true}),(b {x: false})]" +
          "expected[]")

      val left = loader.getLogicalGraphByVariable("g")
        .vertexInducedSubgraph(FilterExpressions.hasProperty("x", PropertyValue(true)))
      val right = loader.getLogicalGraphByVariable("g")
        .vertexInducedSubgraph(FilterExpressions.hasProperty("x", PropertyValue(false)))
      val expected = loader.getLogicalGraphByVariable("expected")

      assert(runOverlap(left, right).equalsByElementData(expected))
    }

    it("Total overlapping graphs", OperatorTest) {
      val loader = getSocialNetworkLoader
      val left = loader.getLogicalGraphByVariable("g0")

      assert(runOverlap(left, left).equalsByElementData(left))
    }

    it("Vertex only overlapping graphs", OperatorTest) {
      val loader = SparkAsciiGraphLoader.fromString(gveConfig,
        "g1[(a)-[e1]->(b)]" +
          "g2[(a)-[e2]->(b)]" +
          "expected[(a)(b)]")

      val left = loader.getLogicalGraphByVariable("g1")
      val right = loader.getLogicalGraphByVariable("g2")
      val expected = loader.getLogicalGraphByVariable("expected")

      assert(runOverlap(left, right).equalsByData(expected))
    }
  }
}
