package org.gradoop.spark.model.impl.operators.changelayout

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.BaseGraph
import org.gradoop.spark.model.api.layouts.gve.GveBaseLayoutFactory
import org.gradoop.spark.model.api.operators.{UnaryGraphCollectionToValueOperator, UnaryLogicalGraphToValueOperator}
import org.gradoop.spark.model.impl.types.{Gve, Tfl}
import org.gradoop.spark.util.TflFunctions

class TflToGve[L1 <: Tfl[L1], L2 <: Gve[L2]](gveConfig: GradoopSparkConfig[L2])
  extends UnaryLogicalGraphToValueOperator[L1#LG, L2#LG] with UnaryGraphCollectionToValueOperator[L1#GC, L2#GC] {

  override def execute(graph: L1#LG): L2#LG = {
    toGve(graph.layout, gveConfig.logicalGraphFactory)
  }

  override def execute(graphCollection: L1#GC): L2#GC = {
    toGve(graphCollection.layout, gveConfig.graphCollectionFactory)
  }

  private def toGve[BG <: BaseGraph[L2]](layout: L1#L, gveFactory: GveBaseLayoutFactory[L2, BG]): BG = {
    import gveFactory.Implicits._
    import gveConfig.Implicits._

    gveFactory.init(
      TflFunctions.reduceUnion(layout.graphHeadsWithProperties.values.map(_.as[L2#G])),
      TflFunctions.reduceUnion(layout.verticesWithProperties.values.map(_.as[L2#V])),
      TflFunctions.reduceUnion(layout.edgesWithProperties.values.map(_.as[L2#E]))
    )
  }
}
