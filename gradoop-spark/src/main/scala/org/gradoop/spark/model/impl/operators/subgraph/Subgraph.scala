package org.gradoop.spark.model.impl.operators.subgraph

import org.gradoop.spark.model.api.operators.UnaryLogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.types.LayoutType

abstract class Subgraph[L <: LayoutType[L]] extends UnaryLogicalGraphToLogicalGraphOperator[L#LG]

