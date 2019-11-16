package org.gradoop.spark.model.api.layouts

import org.gradoop.common.model.api.elements.ElementFactoryProvider
import org.gradoop.spark.model.impl.types.GveGraphLayout

trait BaseLayoutFactory[L <: GveGraphLayout] extends ElementEncoderProvider[L#G, L#V, L#E]
  with ElementFactoryProvider[L#G, L#V, L#E] {
}