package org.gradoop.common.model.api.components

import org.gradoop.common.id.GradoopId
import org.gradoop.common.properties.PropertyValue

trait ComponentTypes {
  type Label = String//Array[String]
  type Id = GradoopId
  type IdSet = Set[Id]
  type Properties = Map[String, PropertyValue]
}
