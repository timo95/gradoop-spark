package org.gradoop.spark.model.impl.operators.changelayout

import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.operators.UnaryLogicalGraphToValueOperator
import org.gradoop.spark.model.impl.types.{Gve, Tfl}

class TflToGve[L1 <: Tfl[L1], L2 <: Gve[L2]](gveConfig: GradoopSparkConfig[L2])
  extends UnaryLogicalGraphToValueOperator[L1#LG, L2#LG] {

  override def execute(graph: L1#LG): L2#LG = {
    val factory = graph.factory
    import factory.Implicits._

    val graphHeads = graph.graphHeads
      .map(g => (g._1, g._2.join(graph.graphHeadProperties(g._1), Seq(ColumnNames.ID, ColumnNames.LABEL))))
    val vertices = graph.vertices
      .map(v => (v._1, v._2.join(graph.vertexProperties(v._1), Seq(ColumnNames.ID, ColumnNames.LABEL))))
    val edges = graph.edges
      .map(e => (e._1, e._2.join(graph.edgeProperties(e._1), Seq(ColumnNames.ID, ColumnNames.LABEL))))

    val factory2 = gveConfig.logicalGraphFactory
    import factory2.Implicits._
    factory2.init(
      graphHeads.values.reduce(_ union _).as[L2#G],
      vertices.values.reduce(_ union _).as[L2#V],
      edges.values.reduce(_ union _).as[L2#E]
    )
  }
}
