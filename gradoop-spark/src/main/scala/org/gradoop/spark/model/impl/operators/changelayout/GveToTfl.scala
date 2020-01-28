package org.gradoop.spark.model.impl.operators.changelayout

import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.expressions.filter.FilterExpressions
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.operators.UnaryLogicalGraphToValueOperator
import org.gradoop.spark.model.impl.types.{Gve, Tfl}

class GveToTfl[L1 <: Gve[L1], L2 <: Tfl[L2]](tflConfig: GradoopSparkConfig[L2])
  extends UnaryLogicalGraphToValueOperator[L1#LG, L2#LG] {

  override def execute(graph: L1#LG): L2#LG = {
    import graph.config.sparkSession.implicits._
    import org.apache.spark.sql.functions._

    // Extract labels
    val factory = graph.factory
    import factory.Implicits._
    val graphLabels = graph.graphHead.select(graph.graphHead.label).distinct.collect
    val vertexLabels = graph.vertices.select(graph.vertices.label).distinct.collect
    val edgeLabels = graph.edges.select(graph.edges.label).distinct.collect

    // Split datasets by labels
    val graphHeads = graphLabels
      .map(l => (l, graph.graphHeads.filter(FilterExpressions.hasLabel(l)))).toMap
    val vertices = vertexLabels
      .map(l => (l, graph.vertices.filter(FilterExpressions.hasLabel(l)))).toMap
    val edges = edgeLabels
      .map(l => (l, graph.edges.filter(FilterExpressions.hasLabel(l)))).toMap

    { // Put tfl implicits in scope to prefer them over gve implicits (above)
      val factory2 = tflConfig.logicalGraphFactory
      import factory2.Implicits._

      // Split maps in main element and property. Use constant as label.
      val resGrap = graphHeads
        .map(g => (g._1, g._2.select(g._2.id, lit(g._1).as(ColumnNames.LABEL)).as[L2#G]))
      val resGrapProp = graphHeads
        .map(p => (p._1, p._2.select(p._2.id, lit(p._1).as(ColumnNames.LABEL), p._2.properties).as[L2#P]))
      val resVert = vertices
        .map(v => (v._1, v._2.select(v._2.id, lit(v._1).as(ColumnNames.LABEL), v._2.graphIds).as[L2#V]))
      val resVertProp = vertices
        .map(p => (p._1, p._2.select(p._2.id, lit(p._1).as(ColumnNames.LABEL), p._2.properties).as[L2#P]))
      val resEdge = edges
        .map(e => (e._1, e._2.select(e._2.id, lit(e._1).as(ColumnNames.LABEL), e._2.sourceId, e._2.targetId,
          e._2.graphIds).as[L2#E]))
      val resEdgeProp = edges
        .map(p => (p._1, p._2.select(p._2.id, lit(p._1).as(ColumnNames.LABEL), p._2.properties).as[L2#P]))

      factory2.init(resGrap, resVert, resEdge, resGrapProp, resVertProp, resEdgeProp)
    }
  }
}
