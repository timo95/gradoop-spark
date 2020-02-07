package org.gradoop.spark.model.impl.operators.setgraph

import org.gradoop.common.properties.PropertyValue
import org.gradoop.spark.expressions.FilterExpressions
import org.gradoop.spark.model.impl.operators.tostring.gve.{CanonicalAdjacencyMatrixBuilder, ElementToString}
import org.gradoop.spark.util.SparkAsciiGraphLoader
import org.gradoop.spark.{EpgmGradoopSparkTestBase, OperatorTest}
import org.scalatest.FunSpec

trait ExclusionBehaviors extends EpgmGradoopSparkTestBase {
  this: FunSpec =>

  def exclusion(runExclusion: (L#LG, L#LG) => L#LG) {
    it("Overlapping graphs", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected1:Community {interest:\"Databases\", vertexCount:3}" +
        "[(eve)]" +
        "expected2:Community {interest : \"Graphs\", vertexCount : 4}" +
        "[(carol)-[ckd]->(dave)-[dkc]->(carol)]")

      val left = loader.getLogicalGraphByVariable("g0")
      val right = loader.getLogicalGraphByVariable("g2")
      val expected1 = loader.getLogicalGraphByVariable("expected1")
      val expected2 = loader.getLogicalGraphByVariable("expected2")

      assert(runExclusion(left, right).equalsByData(expected1))
      assert(runExclusion(right, left).equalsByData(expected2))
    }

    it("Overlapping graphs derived", OperatorTest) {
      val loader = SparkAsciiGraphLoader.fromString(gveConfig,
        "g:test[(a {x: true, y: true})(b {x:true, y: false})]" +
          "expected:test[(a)]")

      val left = loader.getLogicalGraphByVariable("g")
        .vertexInducedSubgraph(FilterExpressions.hasProperty("x", PropertyValue(true)))
      val right = loader.getLogicalGraphByVariable("g")
        .vertexInducedSubgraph(FilterExpressions.hasProperty("y", PropertyValue(false)))
      val expected = loader.getLogicalGraphByVariable("expected")

      assert(runExclusion(left, right).equalsByData(expected))
    }

    it("Non overlapping graphs", OperatorTest) {
      val loader = getSocialNetworkLoader

      val left = loader.getLogicalGraphByVariable("g0")
      val right = loader.getLogicalGraphByVariable("g1")

      assert(runExclusion(left, right).equalsByData(left))
      assert(runExclusion(right, left).equalsByData(right))
    }

    it("Non overlapping graphs derived", OperatorTest) {
      val loader = SparkAsciiGraphLoader.fromString(gveConfig,
        "g:test[(a {x: true})(b {x: false})]" +
          "expected:test[(a)]")

      val left = loader.getLogicalGraphByVariable("g")
        .vertexInducedSubgraph(FilterExpressions.hasProperty("x", PropertyValue(true)))
      val right = loader.getLogicalGraphByVariable("g")
        .vertexInducedSubgraph(FilterExpressions.hasProperty("x", PropertyValue(false)))
      val expected = loader.getLogicalGraphByVariable("expected")

      assert(runExclusion(left, right).equalsByData(expected))
    }

    it("Total overlapping graphs", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected:Community {interest:\"Databases\", vertexCount:3}[]")

      val left = loader.getLogicalGraphByVariable("g0")
      val expected = loader.getLogicalGraphByVariable("expected")

      assert(runExclusion(left, left).equalsByData(expected))
    }
  }
}
