package org.gradoop.common.model.api.elements

trait Labeled extends Serializable {
  def getLabels: Labels

  def setLabels(labels: Labels): Unit
}
