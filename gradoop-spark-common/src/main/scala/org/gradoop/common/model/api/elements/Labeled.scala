package org.gradoop.common.model.api.elements

trait Labeled extends Serializable {
  def label: Label

  def label_=(labels: Label): Unit
}
