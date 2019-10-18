package org.gradoop.spark.model.api.graph

import org.apache.spark.sql.Dataset
import org.gradoop.common.config.GradoopConfig
import org.gradoop.spark.model.api.GraphModel
import org.gradoop.spark.model.api.layouts.GraphCollectionLayout

trait GraphCollection extends BaseGraph with GraphCollectionLayout {
}
