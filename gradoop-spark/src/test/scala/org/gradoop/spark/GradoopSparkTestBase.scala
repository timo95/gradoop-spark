package org.gradoop.spark

import org.gradoop.spark.model.impl.types.Gve
import org.scalatest.FunSpec

trait GradoopSparkTestBase[L <: Gve[L]] extends FunSpec with GradoopSparkTestUtilities[L]
