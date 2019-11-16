package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Dataset
import org.gradoop.spark.functions.filter.HasLabel
import org.gradoop.spark.model.impl.types.GveGraphLayout

abstract class GveLayout[L <: GveGraphLayout](val graphHeads: Dataset[L#G], val vertices: Dataset[L#V], val edges: Dataset[L#E])
  extends GraphCollectionLayout[L] with LogicalGraphLayout[L] {

  override def graphHead: Dataset[L#G] = graphHeads
  override def graphHeadsByLabel(label: String): Dataset[L#G] = graphHeads.filter(new HasLabel[L#G](label))
  override def verticesByLabel(label: String): Dataset[L#V] = vertices.filter(new HasLabel[L#V](label))
  override def edgesByLabel(label: String): Dataset[L#E] = edges.filter(new HasLabel[L#E](label))
}
