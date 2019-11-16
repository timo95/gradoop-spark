package org.gradoop.spark.model.impl.operators.grouping

import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}
import org.gradoop.spark.model.api.operators.LogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.types.GveGraphLayout

class Grouping[L <: GveGraphLayout] extends LogicalGraphToLogicalGraphOperator[LogicalGraph[L]] {
  type LG = LogicalGraph[L]

  override def execute(graph: LG): LG = {
    val config = graph.config
    val session = config.sparkSession

    import config.implicits._
    import session.implicits._


    graph

    // Vertex: Translate to tuples
    // Vertex: Group by Keys
    // Vertex: Reduce Group with Aggregation, determine SuperVertex per Group and retain all elements
    // Vertex: extract Mapping: VertexID -> SuperVertexId

    // Edge: Translate to tuples
    // Edge: Update with mapping
    // Edge: Group by Keys and Source/TargetIDs
    // Edge: Reduce Group with Aggregation, determine SuperEdge per Group

    // Vertex: Filter for Super Vertices

    // Both: Translate back to Objects


    // Alternative?:

    // Vertex: Translate to tuples
    // Vertex: Group by Keys (create key id here?)
    // Vertex: extract Mapping: VertexID -> SuperVertexId
    // Vertex: Reduce Group with Aggregation, determine SuperVertex per Group

    // Edge: Translate to tuples
    // Edge: Update with mapping
    // Edge: Group by Keys and Source/TargetIDs
    // Edge: Reduce Group with Aggregation, determine SuperEdge per Group

    // Vertex: Filter for Super Vertices


  }

}
