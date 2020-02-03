package org.gradoop.spark.model.api.operators

import org.gradoop.spark.model.api.graph.GraphCollection

trait BinaryGraphCollectionToGraphCollectionOperator[GC <: GraphCollection[_]]
  extends BinaryGraphCollectionToValueOperator[GC, GC]
