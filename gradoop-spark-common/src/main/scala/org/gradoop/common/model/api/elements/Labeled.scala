package org.gradoop.common.model.api.elements

trait Labeled extends Serializable {
  def labels: Labels

  def labels_=(labels: Labels): Unit
}
