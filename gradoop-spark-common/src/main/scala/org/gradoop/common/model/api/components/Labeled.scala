package org.gradoop.common.model.api.components

trait Labeled extends Serializable {

  def label: Label

  def label_=(labels: Label): Unit
}
