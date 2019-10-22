package org.gradoop.spark.model.api.types

import org.gradoop.spark.util.GradoopId

trait ComponentTypes {
  type Labels = Array[String]
  type Id = GradoopId //Array[Byte]
  type IdSet = Seq[Id]
  type Properties = String
}
