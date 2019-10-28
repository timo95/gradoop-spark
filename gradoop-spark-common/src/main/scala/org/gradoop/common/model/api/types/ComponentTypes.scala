package org.gradoop.common.model.api.types

import org.gradoop.common.model.impl.id.GradoopId

trait ComponentTypes {
  type Labels = Array[String]
  type Id = GradoopId //Array[Byte]
  type IdSet = Set[Id]
  type Properties = String
}
