package org.gradoop.spark.model.impl.elements

import org.apache.spark.sql.types.{ArrayType, DataTypes, StructField, StructType}
import org.apache.spark.sql.{Encoder, Encoders}
import org.gradoop.common.model.api.elements.{GraphHead, GraphHeadFactory}
import org.gradoop.common.model.impl.id.GradoopId

//class EpgmGraphHead(id: Id, labels: Labels, properties: Properties)
  //extends EpgmElement(id, labels, properties) with GraphHead

case class EpgmGraphHead(var id: Id, var labels: Labels, var properties: Properties) extends GraphHead {
  override def getId: Id = id
  override def getLabels: Labels = labels
  override def getProperties: Properties = properties
  override def setId(id: Id): Unit = this.id = id
  override def setLabels(labels: Labels): Unit = this.labels = labels
  override def setProperties(properties: Properties): Unit = this.properties = properties
}

object EpgmGraphHead extends GraphHeadFactory[G] {

  def getEncoder: Encoder[EpgmGraphHead] = Encoders.kryo(classOf[EpgmGraphHead])

  override def getType: Class[G] = classOf[EpgmGraphHead]

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
  override def apply(id: Id): G = apply(id, new Labels(""))

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
  override def apply(id: Id, labels: Labels): G = apply(id, labels, Map[String, PV]())

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