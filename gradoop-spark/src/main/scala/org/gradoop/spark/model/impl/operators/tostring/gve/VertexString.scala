package org.gradoop.spark.model.impl.operators.tostring.gve

import org.gradoop.common.model.impl.id.GradoopId

final case class VertexString(graphId: GradoopId, id: GradoopId, var string: String) extends ElementString
