package org.gradoop.spark.model.impl.operators.grouping

import org.gradoop.spark.EpgmGradoopSparkTestBase
import org.gradoop.spark.expressions.{AggregateExpressions, GroupingKeyExpressions}
import org.scalatest.FunSpec

trait GroupingBehaviors extends EpgmGradoopSparkTestBase {
  this: FunSpec =>

  def grouping(runGrouping: (L#LG, GroupingBuilder) => L#LG): Unit = {
    it("testVertexPropertySymmetricGraph") {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected[" +
        "(leipzig {city : \"Leipzig\", count : 2L})" +
        "(dresden {city : \"Dresden\", count : 2L})" +
        "(leipzig)-[{count : 2L}]->(leipzig)" +
        "(leipzig)-[{count : 1L}]->(dresden)" +
        "(dresden)-[{count : 2L}]->(dresden)" +
        "(dresden)-[{count : 1L}]->(leipzig)" +
        "]")

      val graph = loader.getLogicalGraphByVariable("g2")
      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(GroupingKeyExpressions.property("city"))
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.count)
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.count)

      assert(graph.groupBy(groupingBuilder).equalsByData(expected))
    }
  }
}
