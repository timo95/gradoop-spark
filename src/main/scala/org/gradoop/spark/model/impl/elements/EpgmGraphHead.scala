package org.gradoop.spark.model.impl.elements

import org.gradoop.spark.model.api.elements.{GraphHead, GraphHeadFactory}
import org.gradoop.spark.util.GradoopId

class EpgmGraphHead(id: Id, labels: Labels, properties: Properties)
  extends EpgmElement(id, labels, properties) with GraphHead

object EpgmGraphHead extends GraphHeadFactory[G] {

  /**
   * Creates a new edge based on the given parameters.
   *
   * @return edge data
   */
  override def create: G = apply(GradoopId.get)

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @return edge data
   */
  override def apply(id: Id): G = apply(id, new Labels(0))

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param labels          edge labels
   * @return edge data
   */
  override def create(labels: Labels): G = apply(GradoopId.get, labels)

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param labels          edge labels
   * @return edge data
   */
  override def apply(id: Id, labels: Labels): G = apply(id, labels, null, null)

  /**
   * Creates a new edge based on the given parameters.
   *
   * @param labels          edge labels
   * @param properties     edge properties
   * @return edge data
   */
  override def create(labels: Labels, properties: Properties): G = apply(GradoopId.get, labels, properties)

  /**
   * Initializes an edge based on the given parameters.
   *
   * @param id             edge identifier
   * @param labels          edge labels
   * @param properties     edge properties
   * @return edge data
   */
  override def apply(id: Id, labels: Labels, properties: Properties): G = new EpgmGraphHead(id, labels, properties)
}