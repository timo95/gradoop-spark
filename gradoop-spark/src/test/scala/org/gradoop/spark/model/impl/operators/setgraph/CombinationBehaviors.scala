package org.gradoop.spark.model.impl.operators.setgraph

import org.gradoop.common.properties.PropertyValue
import org.gradoop.spark.expressions.filter.FilterExpressions
import org.gradoop.spark.model.impl.operators.tostring.gve.{CanonicalAdjacencyMatrixBuilder, ElementToString}
import org.gradoop.spark.util.SparkAsciiGraphLoader
import org.gradoop.spark.{EpgmGradoopSparkTestBase, OperatorTest}
import org.scalatest.FunSpec

trait CombinationBehaviors extends EpgmGradoopSparkTestBase {
  this: FunSpec =>

  def combination(runCombination: (L#LG, L#LG) => L#LG): Unit = {

    it("Overlapping graphs", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected[" +
        "(alice)-[akb]->(bob)" +
        "(bob)-[bka]->(alice)" +
        "(bob)-[bkc]->(carol)" +
        "(carol)-[ckb]->(bob)" +
        "(carol)-[ckd]->(dave)" +
        "(dave)-[dkc]->(carol)" +
        "(eve)-[eka]->(alice)" +
        "(eve)-[ekb]->(bob)]")

      val left = loader.getLogicalGraphByVariable("g0")
      val right = loader.getLogicalGraphByVariable("g2")
      val expected = loader.getLogicalGraphByVariable("expected")

      val mb = new CanonicalAdjacencyMatrixBuilder[L](ElementToString.graphHeadToEmptyString,
        ElementToString.vertexToDataString,
        ElementToString.edgeToDataString, true)

      assert(runCombination(left, right).equalsByElementData(expected))
      assert(runCombination(right, left).equalsByElementData(expected))
    }

    it("Overlapping graphs derived", OperatorTest) {
      val loader = SparkAsciiGraphLoader.fromString(gveConfig,
        "g[(a {x: true, y: true})" +
          "(b {x: true, y: false})]" +
          "expected[(a)(b)]")

      val left = loader.getLogicalGraphByVariable("g")
        .vertexInducedSubgraph(FilterExpressions.hasProperty("x", PropertyValue(true)))
      val right = loader.getLogicalGraphByVariable("g")
        .vertexInducedSubgraph(FilterExpressions.hasProperty("y", PropertyValue(true)))
      val expected = loader.getLogicalGraphByVariable("expected")

      val mb = new CanonicalAdjacencyMatrixBuilder[L](ElementToString.graphHeadToEmptyString,
        ElementToString.vertexToDataString,
        ElementToString.edgeToDataString, true)

      assert(runCombination(left, right).equalsByElementData(expected))
      assert(runCombination(right, left).equalsByElementData(expected))
    }

    it("Non overlapping graphs", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected[" +
        "(alice)-[akb]->(bob)" +
        "(bob)-[bka]->(alice)" +
        "(eve)-[eka]->(alice)" +
        "(eve)-[ekb]->(bob)" +
        "(carol)-[ckd]->(dave)" +
        "(dave)-[dkc]->(carol)" +
        "(frank)-[fkc]->(carol)" +
        "(frank)-[fkd]->(dave)]")

      val left = loader.getLogicalGraphByVariable("g0")
      val right = loader.getLogicalGraphByVariable("g1")
      val expected = loader.getLogicalGraphByVariable("expected")

      val mb = new CanonicalAdjacencyMatrixBuilder[L](ElementToString.graphHeadToEmptyString,
        ElementToString.vertexToDataString,
        ElementToString.edgeToDataString, true)

      assert(runCombination(left, right).equalsByElementData(expected))
      assert(runCombination(right, left).equalsByElementData(expected))
    }

    it("Total overlapping graphs", OperatorTest) {
      val loader = getSocialNetworkLoader
      val left = loader.getLogicalGraphByVariable("g0")

      val mb = new CanonicalAdjacencyMatrixBuilder[L](ElementToString.graphHeadToEmptyString,
        ElementToString.vertexToDataString,
        ElementToString.edgeToDataString, true)

      assert(runCombination(left, left).equalsByElementData(left))
    }
  }
}
