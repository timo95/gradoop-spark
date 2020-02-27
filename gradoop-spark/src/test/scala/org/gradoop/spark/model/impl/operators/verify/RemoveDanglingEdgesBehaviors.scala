package org.gradoop.spark.model.impl.operators.verify

import org.apache.spark.sql.functions._
import org.gradoop.common.properties.PropertyValue
import org.gradoop.spark.expressions.FilterExpressions
import org.gradoop.spark.{EpgmGradoopSparkTestBase, OperatorTest}
import org.scalatest.FunSpec


trait RemoveDanglingEdgesBehaviors extends EpgmGradoopSparkTestBase {
  this: FunSpec =>

  def removeDanglingEdges(runRemoveDanglingEdges: LGve#LG => LGve#LG): Unit = {
    it("Remove dangling edges with Subgraph", OperatorTest) {
      val loader = getSocialNetworkLoader
      loader.appendToDatabaseFromString("expected:Community {interest : \"Databases\", vertexCount : 3}[" +
        "(eve)-[ekb:knows {since : 2015}]->(bob)]")
      val input = loader.getLogicalGraphByVariable("g0")

      // Apply a subgraph operator that would result in dangling edges.
      val subgraph = input.subgraph(
        not(FilterExpressions.hasProperty("name", PropertyValue("Alice"))),
        FilterExpressions.any)

      // Make sure that the graph contains dangling edges.
      val danglingEdges = getDanglingEdges(subgraph).toSet
      val expectedDanglingEdges = Seq(loader.edgeByVariable("eka"),
        loader.edgeByVariable("akb"),
        loader.edgeByVariable("bka")).flatten.toSet

      assert(danglingEdges == expectedDanglingEdges)

      // Now run verify and check if those edges were removed.
      val verifiedSubgraph = runRemoveDanglingEdges(subgraph)
      assert(getDanglingEdges(verifiedSubgraph).isEmpty, "Graph contained dangling edges.")
      // Check if nothing else has been removed (i.e. the result is correct)
      assert(loader.getLogicalGraphByVariable("expected").equalsByData(verifiedSubgraph))
    }
  }

  private def getDanglingEdges(graph: LGve#LG): Seq[LGve#E] = {
    import graph.config.Implicits._
    val ids = graph.vertices.collect.map(_.id)
    graph.edges.collect.filter(e => !ids.contains(e.sourceId) || !ids.contains(e.targetId))
  }
}
