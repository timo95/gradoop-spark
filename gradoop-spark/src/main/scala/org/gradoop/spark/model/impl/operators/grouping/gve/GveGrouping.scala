package org.gradoop.spark.model.impl.operators.grouping.gve

import org.apache.spark.sql.Column
import org.gradoop.common.id.GradoopId
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.{ColumnNames, GradoopConstants}
import org.gradoop.spark.expressions.AggregateExpressions
import org.gradoop.spark.functions.KeyFunction
import org.gradoop.spark.model.api.operators.UnaryLogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.operators.grouping.{GroupingBuilder, GroupingUtil}
import org.gradoop.spark.model.impl.types.Gve

class GveGrouping[L <: Gve[L]](vertexGroupingKeys: Seq[KeyFunction], vertexAggFunctions: Seq[Column],
  edgeGroupingKeys: Seq[KeyFunction], edgeAggFunctions: Seq[Column])
  extends UnaryLogicalGraphToLogicalGraphOperator[L#LG] {

  // Empty key list:
  // vertices: group all
  // edges: group source, target

  // Empty agg list:
  // TODO what about empty agg lists?
  // for now agg by count, but don't save result

  // Column Constants
  val KEYS = "groupingKeys"
  val SUPER_ID = "superId"
  val VERTEX_ID = "vertexId"

  override def execute(graph: L#LG): L#LG = {
    val config = graph.config
    implicit val sparkSession = config.sparkSession
    import org.apache.spark.sql.functions._
    val factory = graph.factory
    import factory.Implicits._

    // UDFs
    val newId = udf(() => GradoopId.get) // Run .cache after each use [SPARK-11469]
    val emptyIdSet = udf(() => Array.empty[GradoopId])

    // ----- Vertices -----

    // Compute vertex grouping keys
    val vertexKeys: Seq[Column] = if(vertexGroupingKeys.isEmpty) Seq(lit(true))
    else vertexGroupingKeys.map(f => f.extractKey.as(f.name))
    val verticesWithKeys = graph.vertices.withColumn(KEYS, struct(vertexKeys: _*))

    // Group and aggregate vertices
    val vertexAggNames = vertexAggFunctions.map(c => GroupingUtil.getAlias(c))
    val vertexAgg = if(vertexAggFunctions.isEmpty) Seq(AggregateExpressions.count) else vertexAggFunctions
    var superVerticesDF = verticesWithKeys.groupBy(KEYS)
      .agg(vertexAgg.head, vertexAgg.drop(1): _*)
      .withColumn(ColumnNames.ID, newId()).cache // Prevents multiple evaluations of newId [SPARK-11469]
      .withColumn(ColumnNames.LABEL, lit(GradoopConstants.DEFAULT_GRAPH_LABEL))
      .withColumn(ColumnNames.GRAPH_IDS, emptyIdSet())

    // Add aggregation result to properties
    if(vertexAggFunctions.isEmpty) {
      superVerticesDF = superVerticesDF
        .withColumn(ColumnNames.PROPERTIES, typedLit(Map.empty[String, PropertyValue]))
        .drop(vertexAgg.map(n => GroupingUtil.getAlias(n)): _*)
    } else {
      superVerticesDF = superVerticesDF
        .withColumn(ColumnNames.PROPERTIES, map_from_arrays(
          array(vertexAggNames.map(n => lit(n)): _*),
          array(vertexAggNames.map(n => col(n)): _*)))
        .drop(vertexAggNames: _*)
    }

    // Add grouping keys to result
    vertexGroupingKeys.foreach(key => {
      superVerticesDF = key.addKeyToElement(superVerticesDF, col(KEYS + "." + key.name))
    })

    // Transform result to vertex
    val superVertices = superVerticesDF
      .drop(KEYS)
      .as[L#V]

    // Extract vertex id -> superId mapping (needed for edges)
    val vertexIdMap = verticesWithKeys.select(KEYS, ColumnNames.ID)
      .join(superVerticesDF.select(col(KEYS), col(ColumnNames.ID).as(SUPER_ID)), KEYS)
      .select(ColumnNames.ID, SUPER_ID)
      .withColumnRenamed(ColumnNames.ID, VERTEX_ID)

    // ----- Edges -----

    // Compute edge grouping keys (might depend on original source/target ids)
    val edgeKeys: Seq[Column] = if(edgeGroupingKeys.isEmpty) Seq(lit(true))
    else edgeGroupingKeys.map(f => f.extractKey.as(f.name))
    val edgesWithKeys = graph.edges.withColumn(KEYS, struct(edgeKeys: _*))

    // Update edges with vertex super ids
    val updatedEdges = edgesWithKeys
      .join(vertexIdMap, col(VERTEX_ID) === col(ColumnNames.SOURCE_ID))
      .drop(ColumnNames.SOURCE_ID, VERTEX_ID)
      .withColumnRenamed(SUPER_ID, ColumnNames.SOURCE_ID)
      .join(vertexIdMap, col(VERTEX_ID) === col(ColumnNames.TARGET_ID))
      .drop(ColumnNames.TARGET_ID, VERTEX_ID)
      .withColumnRenamed(SUPER_ID, ColumnNames.TARGET_ID)

    // Group and aggregate edges
    val edgeAggNames = edgeAggFunctions.map(c => GroupingUtil.getAlias(c))
    val edgeAgg = if(edgeAggFunctions.isEmpty) Seq(AggregateExpressions.count) else edgeAggFunctions
    var superEdgesDF = updatedEdges
      .groupBy(KEYS, ColumnNames.SOURCE_ID, ColumnNames.TARGET_ID)
      .agg(edgeAgg.head, edgeAgg.drop(1): _*)
      .withColumn(ColumnNames.ID, newId()).cache // Prevents multiple evaluations of newId [SPARK-11469]
      .withColumn(ColumnNames.LABEL, lit(GradoopConstants.DEFAULT_GRAPH_LABEL))
      .withColumn(ColumnNames.GRAPH_IDS, emptyIdSet())

    // Add aggregation result to properties
    if(edgeAggFunctions.isEmpty) {
      superEdgesDF = superEdgesDF
        .withColumn(ColumnNames.PROPERTIES, typedLit(Map.empty[String, PropertyValue]))
        .drop(edgeAgg.map(n => GroupingUtil.getAlias(n)): _*)
    } else {
      superEdgesDF = superEdgesDF
        .withColumn(ColumnNames.PROPERTIES, map_from_arrays(
          array(edgeAggNames.map(n => lit(n)): _*),
          array(edgeAggNames.map(n => col(n)): _*)))
        .drop(edgeAggNames: _*)
    }

    // Add grouping keys to result
    edgeGroupingKeys.foreach(key => {
      superEdgesDF = key.addKeyToElement(superEdgesDF, col(KEYS + "." + key.name))
    })

    // Transform result to edge
    val superEdges = superEdgesDF
      .drop(KEYS)
      .as[L#E]

    factory.create(superVertices, superEdges)
  }
}

object GveGrouping {

  def apply[L <: Gve[L]](builder: GroupingBuilder): GveGrouping[L] = {
    new GveGrouping[L](builder.vertexGroupingKeys, builder.vertexAggFunctions,
      builder.edgeGroupingKeys, builder.edgeAggFunctions)
  }
}
