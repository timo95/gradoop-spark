package org.gradoop.spark.model.impl.operators.grouping.tfl

import org.apache.spark.sql.{Column, SparkSession}
import org.gradoop.common.util.{ColumnNames, GradoopConstants}
import org.gradoop.spark.functions.aggregation.AggregationFunction
import org.gradoop.spark.functions.{KeyFunction, LabelKeyFunction}
import org.gradoop.spark.model.api.operators.UnaryLogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.operators.grouping.Functions._
import org.gradoop.spark.model.impl.operators.grouping.GroupingBuilder
import org.gradoop.spark.model.impl.operators.grouping.tfl.Functions._
import org.gradoop.spark.model.impl.types.Tfl
import org.gradoop.spark.util.TflFunctions._

class TflGrouping[L <: Tfl[L]](vertexGroupingKeys: Seq[KeyFunction], vertexAggFunctions: Seq[AggregationFunction],
  edgeGroupingKeys: Seq[KeyFunction], edgeAggFunctions: Seq[AggregationFunction])
  extends UnaryLogicalGraphToLogicalGraphOperator[L#LG] {

  override def execute(graph: L#LG): L#LG = {
    val config = graph.config
    implicit val sparkSession: SparkSession = config.sparkSession
    import org.apache.spark.sql.functions._
    val factory = graph.factory
    import factory.Implicits._

    // ----- Vertices -----

    // Compute vertex grouping keys
    val vertexKeys: Seq[Column] = if(vertexGroupingKeys.isEmpty) Seq(lit(true))
    else vertexGroupingKeys.map(f => f.extractKey.as(f.name))
    val verticesWithKeys = graph.verticesWithProperties
      .mapValues(_.withColumn(KEYS, struct(vertexKeys: _*)).cache)

    // Union over labels and group and aggregate
    val vertexAggFinish = if(vertexAggFunctions.isEmpty) Seq(DEFAULT_AGG.aggregate()) else vertexAggFunctions.map(_.aggregate())
    val groupedVertices = reduceUnion(verticesWithKeys.values)
      .groupBy(KEYS)
      .agg(vertexAggFinish.head, vertexAggFinish.drop(1): _*)
      .withColumn(ColumnNames.ID, longToId(monotonically_increasing_id())).cache()

    // Extract vertex id -> superId mapping (needed for edges)
    // The join uses joinWith and alias to enable selfjoin without the optimizer failing
    val unionSuperVertexIds = groupedVertices
      .select(col(KEYS).as("superKeys"), col(ColumnNames.ID).as(SUPER_ID))
    val vertexIdMap = reduceUnion(verticesWithKeys.values.map(
      _.joinWith(unionSuperVertexIds, unionSuperVertexIds("superKeys") === col(KEYS))
        .select(col("_1." + ColumnNames.ID).as(VERTEX_ID), col("_2." + SUPER_ID)))).cache()

    // Add default Label and GraphIds and make Map
    var superVerticesDF = addDefaultColumns(groupedVertices)

    // Add aggregation result to properties
    superVerticesDF = columnsToProperties(superVerticesDF, vertexAggFunctions.map(_.name))

    // Add grouping keys to result
    val vertexLabels = verticesWithKeys.keys.toArray
    for(key <- vertexGroupingKeys) {
      // Add labels to label key functions
      key match {
        case labelFunc: LabelKeyFunction => labelFunc.labelOpt = Some(vertexLabels)
        case _ => // do nothing
      }
      superVerticesDF = key.addKey(superVerticesDF, col(KEYS + "." + key.name))
    }

    // Cache grouping result and add broadcast hint for joins
    superVerticesDF = superVerticesDF.mapValues(df => broadcast(df.cache))

    // Transform result to vertex and property maps
    val (superVertices, superVertexProperties) = splitVertexMap(superVerticesDF.mapValues(_.drop(KEYS)))

    // ----- Edges -----

    // Compute edge grouping keys
    val edgeKeys: Seq[Column] = if(edgeGroupingKeys.isEmpty) Seq(lit(true))
    else edgeGroupingKeys.map(f => f.extractKey.as(f.name))
    val edgesWithKeys = graph.edgesWithProperties
      .mapValues(_.withColumn(KEYS, struct(edgeKeys: _*)))

    // Update edges with vertex super ids
    val updatedEdges = edgesWithKeys.mapValues(
      _.join(vertexIdMap, col(VERTEX_ID) === col(ColumnNames.SOURCE_ID))
        .drop(ColumnNames.SOURCE_ID, VERTEX_ID)
        .withColumnRenamed(SUPER_ID, ColumnNames.SOURCE_ID)
        .join(vertexIdMap, col(VERTEX_ID) === col(ColumnNames.TARGET_ID))
        .drop(ColumnNames.TARGET_ID, VERTEX_ID)
        .withColumnRenamed(SUPER_ID, ColumnNames.TARGET_ID))

    // Union over labels and group and aggregate edges
    val edgeAggFinish = if(edgeAggFunctions.isEmpty) Seq(DEFAULT_AGG.aggregate()) else edgeAggFunctions.map(_.aggregate())
    val groupedEdges = reduceUnion(updatedEdges.values)
      .groupBy(KEYS, ColumnNames.SOURCE_ID, ColumnNames.TARGET_ID)
      .agg(edgeAggFinish.head, edgeAggFinish.drop(1): _*).withColumn(ColumnNames.ID, longToId(monotonically_increasing_id()))

    // Add default ID, Label and GraphIds
    var superEdgesDF = addDefaultColumns(groupedEdges)

    // Add aggregation result to properties
    superEdgesDF = columnsToProperties(superEdgesDF, edgeAggFunctions.map(_.name))

    // Add grouping keys to result
    val edgeLabels = edgesWithKeys.keys.toArray
    for(key <- edgeGroupingKeys) {
      // Add labels to label key functions
      key match {
        case labelFunc: LabelKeyFunction => labelFunc.labelOpt = Some(edgeLabels)
        case _ => // do nothing
      }
      superEdgesDF = key.addKey(superEdgesDF, col(KEYS + "." + key.name))
    }

    // Transform result to vertex and property maps
    val (superEdges, superEdgeProperties) = splitEdgeMap(superEdgesDF.mapValues(_.drop(KEYS)))

    factory.create(superVertices, superEdges, superVertexProperties, superEdgeProperties)
  }
}

object TflGrouping {

  def apply[L <: Tfl[L]](builder: GroupingBuilder): TflGrouping[L] = {
    new TflGrouping[L](builder.vertexGroupingKeys, builder.vertexAggFunctions,
      builder.edgeGroupingKeys, builder.edgeAggFunctions)
  }
}
