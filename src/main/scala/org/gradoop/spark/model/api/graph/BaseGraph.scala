package org.gradoop.spark.model.api.graph

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.operators.BaseGraphOperators

trait BaseGraph extends ElementAccess with BaseGraphOperators {

  var config: GradoopSparkConfig[G, V, E, LG, GC]
}
