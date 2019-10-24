package org.gradoop.spark.model.api.graph

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.operators.BaseGraphOperators
import org.gradoop.spark.model.api.types.GraphModel

abstract class BaseGraph extends ElementAccess with BaseGraphOperators with GraphModel {

  var config: GradoopSparkConfig[G, V, E, LG, GC]
}
