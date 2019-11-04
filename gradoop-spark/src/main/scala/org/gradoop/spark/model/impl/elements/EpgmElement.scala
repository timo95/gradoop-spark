package org.gradoop.spark.model.impl.elements

import org.gradoop.common.model.api.elements.Element

abstract class EpgmElement(var id: Id, var labels: Labels, var properties: Properties) extends Element {
  override def getId: Id = id
  override def getLabels: Labels = labels
  override def getProperties: Properties = properties

  override def setId(id: Id): Unit = {
    this.id = id
  }

  override def setLabels(labels: Labels): Unit = {
    this.labels = labels
  }

  override def setProperties(properties: Properties): Unit = {
    this.properties = properties
  }
}
