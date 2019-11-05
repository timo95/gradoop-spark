package org.gradoop.spark.model.api.layouts

import org.gradoop.common.model.api.elements.{Edge, ElementFactoryProvider, GraphHead, Vertex}

trait BaseLayoutFactory[G <: GraphHead, V <: Vertex, E <: Edge] extends ElementEncoderProvider[G, V, E]
  with ElementFactoryProvider[G, V, E]