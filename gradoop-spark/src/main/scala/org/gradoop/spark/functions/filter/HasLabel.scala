package org.gradoop.spark.functions.filter

import org.apache.spark.api.java.function.FilterFunction
import org.gradoop.common.model.api.elements.Labeled

class HasLabel[EL <: Labeled](label: String) extends FilterFunction[EL] {
  override def call(element: EL): Boolean = element.label.contains(label)
}
