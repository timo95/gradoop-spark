package org.gradoop.spark.model.impl.operators.changelayout

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.operators.{UnaryGraphCollectionToValueOperator, UnaryLogicalGraphToValueOperator}
import org.gradoop.spark.model.impl.types.{Gve, Tfl}
import org.gradoop.spark.util.TflFunctions

class TflToGve[L1 <: Tfl[L1], L2 <: Gve[L2]](gveConfig: GradoopSparkConfig[L2])
  extends UnaryLogicalGraphToValueOperator[L1#LG, L2#LG] with UnaryGraphCollectionToValueOperator[L1#GC, L2#GC] {
  implicit val sparkSession = gveConfig.sparkSession

  override def execute(graph: L1#LG): L2#LG = {
    val factory = gveConfig.logicalGraphFactory
    import factory.Implicits._
    factory.init(
      TflFunctions.reduceUnion(graph.graphHeadsWithProperties.values.map(_.as[L2#G])),
      TflFunctions.reduceUnion(graph.verticesWithProperties.values.map(_.as[L2#V])),
      TflFunctions.reduceUnion(graph.edgesWithProperties.values.map(_.as[L2#E]))
    )
  }

  override def execute(graphCollection: L1#GC): L2#GC = {
    val factory = gveConfig.graphCollectionFactory
    import factory.Implicits._
    factory.init(
      TflFunctions.reduceUnion(graphCollection.graphHeadsWithProperties.values.map(_.as[L2#G])),
      TflFunctions.reduceUnion(graphCollection.verticesWithProperties.values.map(_.as[L2#V])),
      TflFunctions.reduceUnion(graphCollection.edgesWithProperties.values.map(_.as[L2#E]))
    )
  }
}
