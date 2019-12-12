package org.gradoop.spark.model.api.layouts

import org.gradoop.spark.model.impl.types.LayoutType

trait GraphCollectionLayout[L <: LayoutType[L]] extends Layout[L]
