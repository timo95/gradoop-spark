package org.gradoop.spark.model.impl.operators.tostring.gve

import org.gradoop.common.id.GradoopId

final case class GraphHeadString(id: GradoopId, var string: String) extends ElementString
