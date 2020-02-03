package org.gradoop.spark.model.impl.operators.changelayout

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.operators.{UnaryGraphCollectionToValueOperator, UnaryLogicalGraphToValueOperator}
import org.gradoop.spark.model.impl.types.{Gve, Tfl}

class TflToGve[L1 <: Tfl[L1], L2 <: Gve[L2]](gveConfig: GradoopSparkConfig[L2])
  extends UnaryLogicalGraphToValueOperator[L1#LG, L2#LG] with UnaryGraphCollectionToValueOperator[L1#GC, L2#GC] {

  override def execute(graph: L1#LG): L2#LG = {
    val factory = gveConfig.logicalGraphFactory
    import factory.Implicits._
    factory.init(
      graph.graphHeadsWithProperties.values.reduce(_ union _).as[L2#G],
      graph.verticesWithProperties.values.reduce(_ union _).as[L2#V],
      graph.edgesWithProperties.values.reduce(_ union _).as[L2#E]
    )
  }

  override def execute(graphCollection: L1#GC): L2#GC = {
    val factory = gveConfig.graphCollectionFactory
    import factory.Implicits._
    factory.init(
      graphCollection.graphHeadsWithProperties.values.reduce(_ union _).as[L2#G],
      graphCollection.verticesWithProperties.values.reduce(_ union _).as[L2#V],
      graphCollection.edgesWithProperties.values.reduce(_ union _).as[L2#E]
    )
  }
}
