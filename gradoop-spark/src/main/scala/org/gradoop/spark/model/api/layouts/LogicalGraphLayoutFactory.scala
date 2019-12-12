package org.gradoop.spark.model.api.layouts

import org.gradoop.spark.model.impl.types.LayoutType

trait LogicalGraphLayoutFactory[L <: LayoutType[L]] extends BaseLayoutFactory[L]
