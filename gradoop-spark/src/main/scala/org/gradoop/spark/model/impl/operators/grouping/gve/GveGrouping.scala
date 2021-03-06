package org.gradoop.spark.model.impl.operators.grouping.gve

import org.apache.spark.sql.Column
import org.gradoop.common.util.ColumnNames
import org.gradoop.spark.functions.KeyFunction
import org.gradoop.spark.functions.aggregation.AggregationFunction
import org.gradoop.spark.model.api.operators.UnaryLogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.operators.grouping.Functions._
import org.gradoop.spark.model.impl.operators.grouping.GroupingBuilder
import org.gradoop.spark.model.impl.operators.grouping.gve.Functions._
import org.gradoop.spark.model.impl.types.Gve

/** Gve implementation of the Grouping operator.
 *
 * Behavior for empty key lists:
 * vertices: group all
 * edges: group source, target
 *
 * Behavior for empty aggregation lists:
 * agg by count, but don't save result
 *
 * @param vertexGroupingKeys vertex grouping keys
 * @param vertexAggFunctions vertex aggregation functions
 * @param edgeGroupingKeys edge grouping keys
 * @param edgeAggFunctions edge aggregation functions
 * @tparam L layout type
 */
class GveGrouping[L <: Gve[L]](vertexGroupingKeys: Seq[KeyFunction], vertexAggFunctions: Seq[AggregationFunction],
  edgeGroupingKeys: Seq[KeyFunction], edgeAggFunctions: Seq[AggregationFunction])
  extends UnaryLogicalGraphToLogicalGraphOperator[L#LG] {

  override def execute(graph: L#LG): L#LG = {
    import org.apache.spark.sql.functions._
    val config = graph.config
    import config.Implicits._
    val factory = graph.factory
    import factory.Implicits._

    // ----- Vertices -----

    // Compute vertex grouping keys
    val vertexKeys: Seq[Column] = if(vertexGroupingKeys.isEmpty) Seq(lit(true))
    else vertexGroupingKeys.map(f => f.extractKey.as(f.name))
    val verticesWithKeys = graph.vertices.withColumn(KEYS, struct(vertexKeys: _*)).cache

    // Group and aggregate vertices
    val vertexAgg = if(vertexAggFunctions.isEmpty) Seq(DEFAULT_AGG.aggregate()) else vertexAggFunctions.map(_.aggregate())
    val groupedVertices = verticesWithKeys.groupBy(KEYS)
      .agg(vertexAgg.head, vertexAgg.drop(1): _*)
      .withColumn(ColumnNames.ID, longToId(monotonically_increasing_id())).cache()

    // Extract vertex id -> superId mapping (needed for edges)
    val vertexIdMap = verticesWithKeys.select(KEYS, ColumnNames.ID)
      .join(groupedVertices.select(col(KEYS), col(ColumnNames.ID).as(SUPER_ID)), KEYS)
      .select(col(ColumnNames.ID).as(VERTEX_ID), col(SUPER_ID)).cache()

    // Add default Label and GraphIds
    var superVerticesDF = addDefaultColumns(groupedVertices)

    // Add aggregation result to properties
    superVerticesDF = columnsToProperties(superVerticesDF, vertexAggFunctions.map(_.name))

    // Add grouping keys to result
    for(key <- vertexGroupingKeys) {
      superVerticesDF = key.addKey(superVerticesDF, col(KEYS + "." + key.name))
    }

    // Cache grouping result and add broadcast hint for joins
    superVerticesDF = broadcast(superVerticesDF.cache)

    // Transform result to vertex
    val superVertices = superVerticesDF
      .drop(KEYS)
      .as[L#V]

    // ----- Edges -----

    // Compute edge grouping keys
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
    val edgeAgg = if(edgeAggFunctions.isEmpty) Seq(DEFAULT_AGG.aggregate()) else edgeAggFunctions.map(_.aggregate())
    val groupedEdges = updatedEdges
      .groupBy(KEYS, ColumnNames.SOURCE_ID, ColumnNames.TARGET_ID)
      .agg(edgeAgg.head, edgeAgg.drop(1): _*)
      .withColumn(ColumnNames.ID, longToId(monotonically_increasing_id()))

    // Add default ID, Label and GraphIds
    var superEdgesDF = addDefaultColumns(groupedEdges)

    // Add aggregation result to properties
    superEdgesDF = columnsToProperties(superEdgesDF, edgeAggFunctions.map(_.name))

    // Add grouping keys to result
    for(key <- edgeGroupingKeys) {
      superEdgesDF = key.addKey(superEdgesDF, col(KEYS + "." + key.name))
    }

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
