package org.gradoop.spark.model.api.elements

trait Labeled extends Serializable {
  def getLabels: Labels
}
