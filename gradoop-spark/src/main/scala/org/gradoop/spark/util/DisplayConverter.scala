package org.gradoop.spark.util

import org.apache.spark.sql.{Dataset, Encoder}
import org.gradoop.common.id.GradoopId
import org.gradoop.common.model.api.components.Identifiable

class DisplayConverter[E <: Identifiable](val dataset: Dataset[E]) extends AnyVal {

  /** Workaround to show correct ids when printing the dataset.
   *
   * This is necessary, because 'Dataset#show' does not use 'toString' of GradoopId.
   *
   * @param encoder encoder of the element
   */
  def display()(implicit encoder: Encoder[E]): Unit = {
    dataset.map(e => {
      e.id = new GradoopId(e.id.toString.getBytes)
      e
    }).show(false)
  }
}
