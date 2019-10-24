package org.gradoop.spark.model.impl.elements

import org.gradoop.common.model.api.elements.Element

abstract class EpgmElement(id: Id, labels: Labels, properties: Properties) extends Element {
  override def getId: Id = id
  override def getLabels: Labels = labels
  override def getProperties: Properties = properties
}
