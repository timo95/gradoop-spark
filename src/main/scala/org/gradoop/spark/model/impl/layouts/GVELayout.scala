package org.gradoop.spark.model.impl.layouts

import org.apache.spark.sql.types.DataTypes._
import org.apache.spark.sql.types._
import org.gradoop.common.model.impl.pojo.EPGMElement
import org.gradoop.spark.model.api.layouts.GraphLayout


class GVELayout extends GraphLayout[EPGMElement] {

  var FIELD_ID = "id"
  var FIELD_GRAPH_IDS = "graphIds"
  var FIELD_SOURCE_ID = "sourceId"
  var FIELD_TARGET_ID = "targetId"
  var FIELD_LABELS = "labels"
  var FIELD_PROPERTIES = "properties"

  var elementSchema: StructType = new StructType()
    .add(FIELD_ID, BinaryType, nullable = false)
    .add(FIELD_LABELS, createArrayType(StringType, false), nullable = false)
    .add(FIELD_PROPERTIES, createMapType(StringType, BinaryType, false), nullable = false)

  var graphElementSchema: StructType = new StructType()
    .add(FIELD_GRAPH_IDS, createArrayType(BinaryType, false), nullable = false)

  var graphHeadSchema: StructType = elementSchema

  var vertexSchema: StructType = graphElementSchema

  var edgeSchema: StructType = graphElementSchema
    .add(FIELD_SOURCE_ID, BinaryType, nullable = false)
    .add(FIELD_TARGET_ID, BinaryType, nullable = false)
}
