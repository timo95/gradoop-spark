package org.gradoop.spark.model.api.layouts

import org.gradoop.spark.model.impl.types.LayoutType

trait LogicalGraphLayout[L <: LayoutType[L]] extends Layout[L]
