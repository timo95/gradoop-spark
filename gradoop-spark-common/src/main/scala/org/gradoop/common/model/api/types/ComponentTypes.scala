package org.gradoop.common.model.api.types

import org.gradoop.common.model.impl.id.GradoopId
import org.gradoop.common.properties.PropertyValue

trait ComponentTypes {
  type Label = String//Array[String]
  type Id = GradoopId //Array[Byte]
  type IdSet = Set[Id]
  type PV = PropertyValue
  type Properties = Map[String, PV]//String
}
