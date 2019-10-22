package org.gradoop.spark.util

class GradoopId extends Serializable {



}

object GradoopId {
  def get: GradoopId = {
    new GradoopId
  }
}