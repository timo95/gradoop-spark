package org.gradoop.spark.model.impl.operators.grouping

import org.gradoop.common.util.GradoopConstants.NULL_STRING
import org.gradoop.spark.expressions.AggregateExpressions
import org.gradoop.spark.functions.{LabelKeyFunction, PropertyKeyFunction}
import org.gradoop.spark.model.impl.operators.tostring.gve.{CanonicalAdjacencyMatrixBuilder, ElementToString}
import org.gradoop.spark.util.SparkAsciiGraphLoader
import org.gradoop.spark.{EpgmGradoopSparkTestBase, OperatorTest}
import org.scalatest.FunSpec

trait GroupingBehaviors extends EpgmGradoopSparkTestBase {
  this: FunSpec =>

  def groupingOnlyProperty(runGrouping: (L#LG, GroupingBuilder) => L#LG): Unit = {
    it("testVertexPropertySymmetricGraph", OperatorTest) {
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
      groupingBuilder.vertexGroupingKeys = Seq(new PropertyKeyFunction("city"))
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.count)
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.count)

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }

    it("testSingleVertexProperty", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected[" +
        "(leipzig {city : \"Leipzig\", count : 2L})" +
        "(dresden {city : \"Dresden\", count : 3L})" +
        "(berlin  {city : \"Berlin\",  count : 1L})" +
        "(dresden)-[{count : 2L}]->(dresden)" +
        "(dresden)-[{count : 3L}]->(leipzig)" +
        "(leipzig)-[{count : 2L}]->(leipzig)" +
        "(leipzig)-[{count : 1L}]->(dresden)" +
        "(berlin)-[{count : 2L}]->(dresden)" +
        "]")

      val graph = loader.getLogicalGraphByVariable("g0")
        .combine(loader.getLogicalGraphByVariable("g1"))
        .combine(loader.getLogicalGraphByVariable("g2"))
      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(new PropertyKeyFunction("city"))
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.count)
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.count)

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }

    it("testMultipleVertexProperties", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected[" +
        "(leipzigF {city : \"Leipzig\", gender : \"f\", count : 1L})" +
        "(leipzigM {city : \"Leipzig\", gender : \"m\", count : 1L})" +
        "(dresdenF {city : \"Dresden\", gender : \"f\", count : 2L})" +
        "(dresdenM {city : \"Dresden\", gender : \"m\", count : 1L})" +
        "(berlinM  {city : \"Berlin\", gender : \"m\",  count : 1L})" +
        "(leipzigF)-[{count : 1L}]->(leipzigM)" +
        "(leipzigM)-[{count : 1L}]->(leipzigF)" +
        "(leipzigM)-[{count : 1L}]->(dresdenF)" +
        "(dresdenF)-[{count : 1L}]->(leipzigF)" +
        "(dresdenF)-[{count : 2L}]->(leipzigM)" +
        "(dresdenF)-[{count : 1L}]->(dresdenM)" +
        "(dresdenM)-[{count : 1L}]->(dresdenF)" +
        "(berlinM)-[{count : 1L}]->(dresdenF)" +
        "(berlinM)-[{count : 1L}]->(dresdenM)" +
        "]")

      val graph = loader.getLogicalGraphByVariable("g0")
        .combine(loader.getLogicalGraphByVariable("g1"))
        .combine(loader.getLogicalGraphByVariable("g2"))
      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(new PropertyKeyFunction("city"),
        new PropertyKeyFunction("gender"))
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.count)
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.count)

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }

    it("testSingleVertexPropertyWithAbsentValue", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected[" +
        "(dresden {city : \"Dresden\", count : 2L})" +
        "(others  {city : " + NULL_STRING + ", count : 1L})" +
        "(others)-[{count : 3L}]->(dresden)" +
        "(dresden)-[{count : 1L}]->(dresden)" +
        "]")

      val graph = loader.getLogicalGraphByVariable("g3")
      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(new PropertyKeyFunction("city"))
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.count)
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.count)

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }

    it("testMultipleVertexPropertiesWithAbsentValue", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected[" +
        "(dresdenF {city : \"Dresden\", gender : \"f\", count : 1L})" +
        "(dresdenM {city : \"Dresden\", gender : \"m\", count : 1L})" +
        "(others  {city : " + NULL_STRING + ", gender : " + NULL_STRING + ", count : 1L})" +
        "(others)-[{count : 2L}]->(dresdenM)" +
        "(others)-[{count : 1L}]->(dresdenF)" +
        "(dresdenF)-[{count : 1L}]->(dresdenM)" +
        "]")

      val graph = loader.getLogicalGraphByVariable("g3")
      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(new PropertyKeyFunction("city"),
        new PropertyKeyFunction("gender"))
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.count)
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.count)

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }

    it("testSingleVertexAndSingleEdgeProperty", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected[" +
        "(leipzig {city : \"Leipzig\", count : 2L})" +
        "(dresden {city : \"Dresden\", count : 3L})" +
        "(berlin  {city : \"Berlin\",  count : 1L})" +
        "(dresden)-[{since : 2014, count : 2L}]->(dresden)" +
        "(dresden)-[{since : 2013, count : 2L}]->(leipzig)" +
        "(dresden)-[{since : 2015, count : 1L}]->(leipzig)" +
        "(leipzig)-[{since : 2014, count : 2L}]->(leipzig)" +
        "(leipzig)-[{since : 2013, count : 1L}]->(dresden)" +
        "(berlin)-[{since : 2015, count : 2L}]->(dresden)" +
        "]")

      val graph = loader.getLogicalGraphByVariable("g0")
        .combine(loader.getLogicalGraphByVariable("g1"))
        .combine(loader.getLogicalGraphByVariable("g2"))
      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(new PropertyKeyFunction("city"))
      groupingBuilder.edgeGroupingKeys = Seq(new PropertyKeyFunction("since"))
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.count)
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.count)

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }

    it("testSingleVertexPropertyAndMultipleEdgeProperties", OperatorTest) {
      val loader = SparkAsciiGraphLoader.fromString(gveConfig, "input[" +
        "(v0 {a : 0,b : 0})" +
        "(v1 {a : 0,b : 1})" +
        "(v2 {a : 0,b : 1})" +
        "(v3 {a : 1,b : 0})" +
        "(v4 {a : 1,b : 1})" +
        "(v5 {a : 1,b : 0})" +
        "(v0)-[{a : 0,b : 1}]->(v1)" +
        "(v0)-[{a : 0,b : 2}]->(v2)" +
        "(v1)-[{a : 0,b : 3}]->(v2)" +
        "(v2)-[{a : 0,b : 2}]->(v3)" +
        "(v2)-[{a : 0,b : 1}]->(v3)" +
        "(v4)-[{a : 1,b : 2}]->(v2)" +
        "(v5)-[{a : 1,b : 3}]->(v2)" +
        "(v3)-[{a : 2,b : 3}]->(v4)" +
        "(v4)-[{a : 2,b : 1}]->(v5)" +
        "(v5)-[{a : 2,b : 0}]->(v3)" +
        "]" +
        "expected[" +
        "(v00 {a : 0,count : 3L})" +
        "(v01 {a : 1,count : 3L})" +
        "(v00)-[{a : 0,b : 1,count : 1L}]->(v00)" +
        "(v00)-[{a : 0,b : 2,count : 1L}]->(v00)" +
        "(v00)-[{a : 0,b : 3,count : 1L}]->(v00)" +
        "(v01)-[{a : 2,b : 0,count : 1L}]->(v01)" +
        "(v01)-[{a : 2,b : 1,count : 1L}]->(v01)" +
        "(v01)-[{a : 2,b : 3,count : 1L}]->(v01)" +
        "(v00)-[{a : 0,b : 1,count : 1L}]->(v01)" +
        "(v00)-[{a : 0,b : 2,count : 1L}]->(v01)" +
        "(v01)-[{a : 1,b : 2,count : 1L}]->(v00)" +
        "(v01)-[{a : 1,b : 3,count : 1L}]->(v00)" +
        "]")

      val graph = loader.getLogicalGraphByVariable("input")
      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(new PropertyKeyFunction("a"))
      groupingBuilder.edgeGroupingKeys = Seq(new PropertyKeyFunction("a"),
        new PropertyKeyFunction("b"))
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.count)
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.count)

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }

    it("testMultipleVertexAndMultipleEdgeProperties", OperatorTest) {
      val loader = SparkAsciiGraphLoader.fromString(gveConfig, "input[" +
        "(v0 {a : 0,b : 0})" +
        "(v1 {a : 0,b : 1})" +
        "(v2 {a : 0,b : 1})" +
        "(v3 {a : 1,b : 0})" +
        "(v4 {a : 1,b : 1})" +
        "(v5 {a : 1,b : 0})" +
        "(v0)-[{a : 0,b : 1}]->(v1)" +
        "(v0)-[{a : 0,b : 2}]->(v2)" +
        "(v1)-[{a : 0,b : 3}]->(v2)" +
        "(v2)-[{a : 0,b : 2}]->(v3)" +
        "(v2)-[{a : 0,b : 1}]->(v3)" +
        "(v4)-[{a : 1,b : 2}]->(v2)" +
        "(v5)-[{a : 1,b : 3}]->(v2)" +
        "(v3)-[{a : 2,b : 3}]->(v4)" +
        "(v4)-[{a : 2,b : 1}]->(v5)" +
        "(v5)-[{a : 2,b : 0}]->(v3)" +
        "]" +
        "expected[" +
        "(v00 {a : 0,b : 0,count : 1L})" +
        "(v01 {a : 0,b : 1,count : 2L})" +
        "(v10 {a : 1,b : 0,count : 2L})" +
        "(v11 {a : 1,b : 1,count : 1L})" +
        "(v00)-[{a : 0,b : 1,count : 1L}]->(v01)" +
        "(v00)-[{a : 0,b : 2,count : 1L}]->(v01)" +
        "(v01)-[{a : 0,b : 3,count : 1L}]->(v01)" +
        "(v01)-[{a : 0,b : 1,count : 1L}]->(v10)" +
        "(v01)-[{a : 0,b : 2,count : 1L}]->(v10)" +
        "(v11)-[{a : 2,b : 1,count : 1L}]->(v10)" +
        "(v10)-[{a : 2,b : 3,count : 1L}]->(v11)" +
        "(v10)-[{a : 2,b : 0,count : 1L}]->(v10)" +
        "(v10)-[{a : 1,b : 3,count : 1L}]->(v01)" +
        "(v11)-[{a : 1,b : 2,count : 1L}]->(v01)" +
        "]")

      val graph = loader.getLogicalGraphByVariable("input")
      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(new PropertyKeyFunction("a"),
        new PropertyKeyFunction("b"))
      groupingBuilder.edgeGroupingKeys = Seq(new PropertyKeyFunction("a"),
        new PropertyKeyFunction("b"))
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.count)
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.count)

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }

    it("testVertexAndEdgePropertyWithAbsentValues", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected[" +
        "(dresden {city : \"Dresden\", count : 2L})" +
        "(others  {city : " + NULL_STRING + ", count : 1L})" +
        "(others)-[{since : 2013, count : 1L}]->(dresden)" +
        "(others)-[{since : " + NULL_STRING + ", count : 2L}]->(dresden)" +
        "(dresden)-[{since : 2014, count : 1L}]->(dresden)" +
        "]")

      val graph = loader.getLogicalGraphByVariable("g3")
      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(new PropertyKeyFunction("city"))
      groupingBuilder.edgeGroupingKeys = Seq(new PropertyKeyFunction("since"))
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.count)
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.count)

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }
  }

  def groupingWithLabel(runGrouping: (L#LG, GroupingBuilder) => L#LG): Unit = {
    it("testVertexLabel", OperatorTest) {
      val loader = getSocialNetworkLoader
      val graph = loader.getLogicalGraph
      loader.appendToDatabaseFromString("expected[" +
        "(p:Person  {count : 6L})" +
        "(t:Tag     {count : 3L})" +
        "(f:Forum   {count : 2L})" +
        "(p)-[{count : 10L}]->(p)" +
        "(f)-[{count :  6L}]->(p)" +
        "(p)-[{count :  4L}]->(t)" +
        "(f)-[{count :  4L}]->(t)" +
        "]")

      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(new LabelKeyFunction)
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.count)
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.count)

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }

    it("testVertexLabelAndSingleVertexProperty", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected[" +
        "(l:Person {city : \"Leipzig\", count : 2L})" +
        "(d:Person {city : \"Dresden\", count : 3L})" +
        "(b:Person {city : \"Berlin\",  count : 1L})" +
        "(d)-[{count : 2L}]->(d)" +
        "(d)-[{count : 3L}]->(l)" +
        "(l)-[{count : 2L}]->(l)" +
        "(l)-[{count : 1L}]->(d)" +
        "(b)-[{count : 2L}]->(d)" +
        "]")

      val graph = loader.getLogicalGraphByVariable("g0")
        .combine(loader.getLogicalGraphByVariable("g1"))
        .combine(loader.getLogicalGraphByVariable("g2"))
      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(new LabelKeyFunction, new PropertyKeyFunction("city"))
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.count)
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.count)

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }

    it("testVertexLabelAndSingleVertexPropertyWithAbsentValue", OperatorTest) {
      val loader = getSocialNetworkLoader
      val graph = loader.getLogicalGraph
      loader.appendToDatabaseFromString("expected[" +
        "(pL:Person {city : \"Leipzig\", count : 2L})" +
        "(pD:Person {city : \"Dresden\", count : 3L})" +
        "(pB:Person {city : \"Berlin\",  count : 1L})" +
        "(t:Tag {city : " + NULL_STRING + ",   count : 3L})" +
        "(f:Forum {city : " + NULL_STRING + ", count : 2L})" +
        "(pD)-[{count : 2L}]->(pD)" +
        "(pD)-[{count : 3L}]->(pL)" +
        "(pL)-[{count : 2L}]->(pL)" +
        "(pL)-[{count : 1L}]->(pD)" +
        "(pB)-[{count : 2L}]->(pD)" +
        "(pB)-[{count : 1L}]->(t)" +
        "(pD)-[{count : 2L}]->(t)" +
        "(pL)-[{count : 1L}]->(t)" +
        "(f)-[{count : 3L}]->(pD)" +
        "(f)-[{count : 3L}]->(pL)" +
        "(f)-[{count : 4L}]->(t)" +
        "]")

      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(new LabelKeyFunction, new PropertyKeyFunction("city"))
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.count)
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.count)

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }

    it("testVertexLabelAndSingleEdgeProperty", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected[" +
        "(p:Person {count : 6L})" +
        "(p)-[{since : 2014, count : 4L}]->(p)" +
        "(p)-[{since : 2013, count : 3L}]->(p)" +
        "(p)-[{since : 2015, count : 3L}]->(p)" +
        "]")

      val graph = loader.getLogicalGraphByVariable("g0")
        .combine(loader.getLogicalGraphByVariable("g1"))
        .combine(loader.getLogicalGraphByVariable("g2"))
      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(new LabelKeyFunction)
      groupingBuilder.edgeGroupingKeys = Seq(new PropertyKeyFunction("since"))
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.count)
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.count)

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }

    it("testVertexLabelAndSingleEdgePropertyWithAbsentValue", OperatorTest) {
      val loader = getSocialNetworkLoader
      val graph = loader.getLogicalGraph
      loader.appendToDatabaseFromString("expected[" +
        "(p:Person  {count : 6L})" +
        "(t:Tag     {count : 3L})" +
        "(f:Forum   {count : 2L})" +
        "(p)-[{since : 2014, count : 4L}]->(p)" +
        "(p)-[{since : 2013, count : 3L}]->(p)" +
        "(p)-[{since : 2015, count : 3L}]->(p)" +
        "(f)-[{since : 2013, count : 1L}]->(p)" +
        "(p)-[{since : " + NULL_STRING + ", count : 4L}]->(t)" +
        "(f)-[{since : " + NULL_STRING + ", count : 4L}]->(t)" +
        "(f)-[{since : " + NULL_STRING + ", count : 5L}]->(p)" +
        "]")

      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(new LabelKeyFunction)
      groupingBuilder.edgeGroupingKeys = Seq(new PropertyKeyFunction("since"))
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.count)
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.count)

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }

    it("testVertexLabelAndSingleVertexAndSingleEdgeProperty", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected[" +
        "(l:Person {city : \"Leipzig\", count : 2L})" +
        "(d:Person {city : \"Dresden\", count : 3L})" +
        "(b:Person {city : \"Berlin\",  count : 1L})" +
        "(d)-[{since : 2014, count : 2L}]->(d)" +
        "(d)-[{since : 2013, count : 2L}]->(l)" +
        "(d)-[{since : 2015, count : 1L}]->(l)" +
        "(l)-[{since : 2014, count : 2L}]->(l)" +
        "(l)-[{since : 2013, count : 1L}]->(d)" +
        "(b)-[{since : 2015, count : 2L}]->(d)" +
        "]")

      val graph = loader.getLogicalGraphByVariable("g0")
        .combine(loader.getLogicalGraphByVariable("g1"))
        .combine(loader.getLogicalGraphByVariable("g2"))
      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(new LabelKeyFunction, new PropertyKeyFunction("city"))
      groupingBuilder.edgeGroupingKeys = Seq(new PropertyKeyFunction("since"))
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.count)
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.count)

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }

    it("testVertexAndEdgeLabel", OperatorTest) {
      val loader = getSocialNetworkLoader
      val graph = loader.getLogicalGraph
      loader.appendToDatabaseFromString("expected[" +
        "(p:Person  {count : 6L})" +
        "(t:Tag     {count : 3L})" +
        "(f:Forum   {count : 2L})" +
        "(f)-[:hasModerator {count :  2L}]->(p)" +
        "(p)-[:hasInterest  {count :  4L}]->(t)" +
        "(f)-[:hasMember    {count :  4L}]->(p)" +
        "(f)-[:hasTag       {count :  4L}]->(t)" +
        "(p)-[:knows        {count : 10L}]->(p)" +
        "]")

      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(new LabelKeyFunction)
      groupingBuilder.edgeGroupingKeys = Seq(new LabelKeyFunction)
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.count)
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.count)

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }

    it("testVertexAndEdgeLabelAndSingleVertexProperty", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected[" +
        "(l:Person {city : \"Leipzig\", count : 2L})" +
        "(d:Person {city : \"Dresden\", count : 3L})" +
        "(b:Person {city : \"Berlin\",  count : 1L})" +
        "(d)-[:knows {count : 2L}]->(d)" +
        "(d)-[:knows {count : 3L}]->(l)" +
        "(l)-[:knows {count : 2L}]->(l)" +
        "(l)-[:knows {count : 1L}]->(d)" +
        "(b)-[:knows {count : 2L}]->(d)" +
        "]")

      val graph = loader.getLogicalGraphByVariable("g0")
        .combine(loader.getLogicalGraphByVariable("g1"))
        .combine(loader.getLogicalGraphByVariable("g2"))
      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(new LabelKeyFunction, new PropertyKeyFunction("city"))
      groupingBuilder.edgeGroupingKeys = Seq(new LabelKeyFunction)
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.count)
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.count)

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }

    it("testVertexAndEdgeLabelAndSingleVertexPropertyWithAbsentValue", OperatorTest) {
      val loader = getSocialNetworkLoader
      val graph = loader.getLogicalGraph
      loader.appendToDatabaseFromString("expected[" +
        "(pL:Person {city : \"Leipzig\", count : 2L})" +
        "(pD:Person {city : \"Dresden\", count : 3L})" +
        "(pB:Person {city : \"Berlin\", count : 1L})" +
        "(t:Tag   {city : " + NULL_STRING + ", count : 3L})" +
        "(f:Forum {city : " + NULL_STRING + ", count : 2L})" +
        "(pD)-[:knows {count : 2L}]->(pD)" +
        "(pD)-[:knows {count : 3L}]->(pL)" +
        "(pL)-[:knows {count : 2L}]->(pL)" +
        "(pL)-[:knows {count : 1L}]->(pD)" +
        "(pB)-[:knows {count : 2L}]->(pD)" +
        "(pB)-[:hasInterest {count : 1L}]->(t)" +
        "(pD)-[:hasInterest {count : 2L}]->(t)" +
        "(pL)-[:hasInterest {count : 1L}]->(t)" +
        "(f)-[:hasModerator {count : 1L}]->(pD)" +
        "(f)-[:hasModerator {count : 1L}]->(pL)" +
        "(f)-[:hasMember {count : 2L}]->(pD)" +
        "(f)-[:hasMember {count : 2L}]->(pL)" +
        "(f)-[:hasTag {count : 4L}]->(t)" +
        "]")

      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(new LabelKeyFunction, new PropertyKeyFunction("city"))
      groupingBuilder.edgeGroupingKeys = Seq(new LabelKeyFunction)
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.count)
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.count)

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }

    it("testVertexAndEdgeLabelAndSingleEdgeProperty", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected[" +
        "(p:Person {count : 6L})" +
        "(p)-[:knows {since : 2013, count : 3L}]->(p)" +
        "(p)-[:knows {since : 2014, count : 4L}]->(p)" +
        "(p)-[:knows {since : 2015, count : 3L}]->(p)" +
        "]")

      val graph = loader.getLogicalGraphByVariable("g0")
        .combine(loader.getLogicalGraphByVariable("g1"))
        .combine(loader.getLogicalGraphByVariable("g2"))
      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(new LabelKeyFunction)
      groupingBuilder.edgeGroupingKeys = Seq(new LabelKeyFunction, new PropertyKeyFunction("since"))
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.count)
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.count)

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }

    it("testVertexAndEdgeLabelAndSingleEdgePropertyWithAbsentValue", OperatorTest) {
      val loader = getSocialNetworkLoader
      val graph = loader.getLogicalGraph
      loader.appendToDatabaseFromString("expected[" +
        "(p:Person  {count : 6L})" +
        "(t:Tag     {count : 3L})" +
        "(f:Forum   {count : 2L})" +
        "(p)-[:knows {since : 2014, count : 4L}]->(p)" +
        "(p)-[:knows {since : 2013, count : 3L}]->(p)" +
        "(p)-[:knows {since : 2015, count : 3L}]->(p)" +
        "(f)-[:hasModerator {since : 2013, count : 1L}]->(p)" +
        "(f)-[:hasModerator {since : " + NULL_STRING + ", count : 1L}]->(p)" +
        "(p)-[:hasInterest  {since : " + NULL_STRING + ", count : 4L}]->(t)" +
        "(f)-[:hasMember    {since : " + NULL_STRING + ", count : 4L}]->(p)" +
        "(f)-[:hasTag       {since : " + NULL_STRING + ", count : 4L}]->(t)" +
        "]")

      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(new LabelKeyFunction)
      groupingBuilder.edgeGroupingKeys = Seq(new LabelKeyFunction, new PropertyKeyFunction("since"))
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.count)
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.count)

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }

    it("testVertexAndEdgeLabelAndVertexAndSingleEdgeProperty", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected[" +
        "(pL:Person {city : \"Leipzig\", count : 2L})" +
        "(pD:Person {city : \"Dresden\", count : 3L})" +
        "(pB:Person {city : \"Berlin\", count : 1L})" +
        "(pD)-[:knows {since : 2014, count : 2L}]->(pD)" +
        "(pD)-[:knows {since : 2013, count : 2L}]->(pL)" +
        "(pD)-[:knows {since : 2015, count : 1L}]->(pL)" +
        "(pL)-[:knows {since : 2014, count : 2L}]->(pL)" +
        "(pL)-[:knows {since : 2013, count : 1L}]->(pD)" +
        "(pB)-[:knows {since : 2015, count : 2L}]->(pD)" +
        "]")

      val graph = loader.getLogicalGraphByVariable("g0")
        .combine(loader.getLogicalGraphByVariable("g1"))
        .combine(loader.getLogicalGraphByVariable("g2"))
      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(new LabelKeyFunction, new PropertyKeyFunction("city"))
      groupingBuilder.edgeGroupingKeys = Seq(new LabelKeyFunction, new PropertyKeyFunction("since"))
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.count)
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.count)

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }

    it("testVertexAndEdgeLabelAndSingleVertexAndSingleEdgePropertyWithAbsentValue", OperatorTest) {
      val loader = getSocialNetworkLoader
      val graph = loader.getLogicalGraph
      loader.appendToDatabaseFromString("expected[" +
        "(pL:Person {city : \"Leipzig\", count : 2L})" +
        "(pD:Person {city : \"Dresden\", count : 3L})" +
        "(pB:Person {city : \"Berlin\", count : 1L})" +
        "(t:Tag   {city : " + NULL_STRING + ", count : 3L})" +
        "(f:Forum {city : " + NULL_STRING + ", count : 2L})" +
        "(pD)-[:knows {since : 2014, count : 2L}]->(pD)" +
        "(pD)-[:knows {since : 2013, count : 2L}]->(pL)" +
        "(pD)-[:knows {since : 2015, count : 1L}]->(pL)" +
        "(pL)-[:knows {since : 2014, count : 2L}]->(pL)" +
        "(pL)-[:knows {since : 2013, count : 1L}]->(pD)" +
        "(pB)-[:knows {since : 2015, count : 2L}]->(pD)" +
        "(pB)-[:hasInterest {since : " + NULL_STRING + ", count : 1L}]->(t)" +
        "(pD)-[:hasInterest {since : " + NULL_STRING + ", count : 2L}]->(t)" +
        "(pL)-[:hasInterest {since : " + NULL_STRING + ", count : 1L}]->(t)" +
        "(f)-[:hasModerator {since : 2013, count : 1L}]->(pD)" +
        "(f)-[:hasModerator {since : " + NULL_STRING + ", count : 1L}]->(pL)" +
        "(f)-[:hasMember {since : " + NULL_STRING + ", count : 2L}]->(pD)" +
        "(f)-[:hasMember {since : " + NULL_STRING + ", count : 2L}]->(pL)" +
        "(f)-[:hasTag {since : " + NULL_STRING + ", count : 4L}]->(t)" +
        "]")

      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(new LabelKeyFunction, new PropertyKeyFunction("city"))
      groupingBuilder.edgeGroupingKeys = Seq(new LabelKeyFunction, new PropertyKeyFunction("since"))
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.count)
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.count)

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }
  }

  def groupingAggFunctions(runGrouping: (L#LG, GroupingBuilder) => L#LG): Unit = {
    it("testNoAggregate", OperatorTest) {
      val loader = SparkAsciiGraphLoader.fromString(gveConfig, "input[" +
        "(v0:Blue {a : 3})" +
        "(v1:Blue {a : 2})" +
        "(v2:Blue {a : 4})" +
        "(v3:Red  {a : 4})" +
        "(v4:Red  {a : 2})" +
        "(v5:Red  {a : 4})" +
        "(v0)-[{b : 2}]->(v1)" +
        "(v0)-[{b : 1}]->(v2)" +
        "(v1)-[{b : 2}]->(v2)" +
        "(v2)-[{b : 3}]->(v3)" +
        "(v2)-[{b : 1}]->(v3)" +
        "(v3)-[{b : 3}]->(v4)" +
        "(v4)-[{b : 1}]->(v5)" +
        "(v5)-[{b : 1}]->(v3)" +
        "]")

      val graph = loader.getLogicalGraphByVariable("input")

      loader.appendToDatabaseFromString("expected[" +
        "(v00:Blue)" +
        "(v01:Red)" +
        "(v00)-->(v00)" +
        "(v00)-->(v01)" +
        "(v01)-->(v01)" +
        "]")

      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(new LabelKeyFunction)

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }

    it("testCount", OperatorTest) {
      val loader = SparkAsciiGraphLoader.fromString(gveConfig, "input[" +
        "(v0:Blue {a : 3})" +
        "(v1:Blue {a : 2})" +
        "(v2:Blue {a : 4})" +
        "(v3:Red  {a : 4})" +
        "(v4:Red  {a : 2})" +
        "(v5:Red  {a : 4})" +
        "(v0)-[{b : 2}]->(v1)" +
        "(v0)-[{b : 1}]->(v2)" +
        "(v1)-[{b : 2}]->(v2)" +
        "(v2)-[{b : 3}]->(v3)" +
        "(v2)-[{b : 1}]->(v3)" +
        "(v3)-[{b : 3}]->(v4)" +
        "(v4)-[{b : 1}]->(v5)" +
        "(v5)-[{b : 1}]->(v3)" +
        "]")

      val graph = loader.getLogicalGraphByVariable("input")

      loader.appendToDatabaseFromString("expected[" +
        "(v00:Blue {count : 3L})" +
        "(v01:Red  {count : 3L})" +
        "(v00)-[{count : 3L}]->(v00)" +
        "(v00)-[{count : 2L}]->(v01)" +
        "(v01)-[{count : 3L}]->(v01)" +
        "]")

      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(new LabelKeyFunction)
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.count)
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.count)

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }

    it("testSum", OperatorTest) {
      val loader = SparkAsciiGraphLoader.fromString(gveConfig, "input[" +
        "(v0:Blue {a : 3})" +
        "(v1:Blue {a : 2})" +
        "(v2:Blue {a : 4})" +
        "(v3:Red  {a : 4})" +
        "(v4:Red  {a : 2})" +
        "(v5:Red  {a : 4})" +
        "(v0)-[{b : 2}]->(v1)" +
        "(v0)-[{b : 1}]->(v2)" +
        "(v1)-[{b : 2}]->(v2)" +
        "(v2)-[{b : 3}]->(v3)" +
        "(v2)-[{b : 1}]->(v3)" +
        "(v3)-[{b : 3}]->(v4)" +
        "(v4)-[{b : 1}]->(v5)" +
        "(v5)-[{b : 1}]->(v3)" +
        "]")

      val graph = loader.getLogicalGraphByVariable("input")

      loader.appendToDatabaseFromString("expected[" +
        "(v00:Blue {sumA :  9})" +
        "(v01:Red  {sumA : 10})" +
        "(v00)-[{sumB : 5}]->(v00)" +
        "(v00)-[{sumB : 4}]->(v01)" +
        "(v01)-[{sumB : 5}]->(v01)" +
        "]")

      val expected = loader.getLogicalGraphByVariable("expected")

      val groupingBuilder = new GroupingBuilder
      groupingBuilder.vertexGroupingKeys = Seq(new LabelKeyFunction)
      groupingBuilder.vertexAggFunctions = Seq(AggregateExpressions.sumProp("a").as("sumA"))
      groupingBuilder.edgeAggFunctions = Seq(AggregateExpressions.sumProp("b").as("sumB"))

      assert(runGrouping(graph, groupingBuilder).equalsByData(expected))
    }

    // TODO add missing aggregation function tests
  }
}
