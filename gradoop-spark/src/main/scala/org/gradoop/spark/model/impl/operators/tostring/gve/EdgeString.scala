package org.gradoop.spark.model.impl.operators.tostring.gve

import org.gradoop.common.model.impl.id.GradoopId

final case class EdgeString(graphId: GradoopId,
                            var sourceId: GradoopId,
                            var targetId: GradoopId,
                            var sourceString: String,
                            var string: String,
                            var targetString: String) extends ElementString
