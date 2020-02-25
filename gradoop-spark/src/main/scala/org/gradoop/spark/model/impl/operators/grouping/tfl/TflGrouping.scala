package org.gradoop.spark.model.impl.operators.grouping.tfl

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.gradoop.common.id.GradoopId
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.{ColumnNames, GradoopConstants}
import org.gradoop.spark.functions.KeyFunction
import org.gradoop.spark.model.api.operators.UnaryLogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.operators.grouping.GroupingBuilder
import org.gradoop.spark.model.impl.operators.grouping.GroupingUtil._
import org.gradoop.spark.model.impl.types.Tfl
import org.gradoop.spark.util.TflFunctions._

class TflGrouping[L <: Tfl[L]](vertexGroupingKeys: Seq[KeyFunction], vertexAggFunctions: Seq[Column],
  edgeGroupingKeys: Seq[KeyFunction], edgeAggFunctions: Seq[Column])
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

    // Group and aggregate vertices
    val vertexAgg = if(vertexAggFunctions.isEmpty) Seq(defaultAgg) else vertexAggFunctions
    var superVerticesDF = Map(GradoopConstants.DEFAULT_GRAPH_LABEL ->
      reduceUnion(verticesWithKeys.values).groupBy(KEYS).agg(vertexAgg.head, vertexAgg.drop(1): _*))

    // Add default ID, Label and GraphIds
    superVerticesDF = addDefaultColumns(superVerticesDF)

    // Add aggregation result to properties
    superVerticesDF = columnsToProperties(superVerticesDF, vertexAggFunctions)

    // Add grouping keys to result
    for(key <- vertexGroupingKeys) {
      superVerticesDF = key.addKey(superVerticesDF, col(KEYS + "." + key.name))
    }

    // Cache grouping result
    superVerticesDF = superVerticesDF.mapValues(_.cache)

    // Transform result to vertex and property maps
    val (superVertices, superVertexProperties) = splitVertexMap(superVerticesDF.mapValues(_.drop(KEYS)))

    // Extract vertex id -> superId mapping (needed for edges)
    // The join uses joinWith and alias to enable selfjoin without the optimizer failing
    val unionSuperVertexIds = reduceUnion(superVerticesDF.values)
      .select(col(KEYS).as("superKeys"), col(ColumnNames.ID).as(SUPER_ID))
    val vertexIdMap = reduceUnion(verticesWithKeys.values.map(
      _.joinWith(unionSuperVertexIds, unionSuperVertexIds("superKeys") === col(KEYS))
        .select(col("_1." + ColumnNames.ID).as(VERTEX_ID), col("_2." + SUPER_ID))))

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

    // Group and aggregate edges
    val edgeAgg = if(edgeAggFunctions.isEmpty) Seq(defaultAgg) else edgeAggFunctions
    var superEdgesDF = Map(GradoopConstants.DEFAULT_GRAPH_LABEL -> reduceUnion(updatedEdges.values)
      .groupBy(KEYS, ColumnNames.SOURCE_ID, ColumnNames.TARGET_ID)
      .agg(edgeAgg.head, edgeAgg.drop(1): _*))

    // Add default ID, Label and GraphIds
    superEdgesDF = addDefaultColumns(superEdgesDF)

    // Add aggregation result to properties
    superEdgesDF = columnsToProperties(superEdgesDF, edgeAggFunctions)

    // Add grouping keys to result
    for(key <- edgeGroupingKeys) {
      superEdgesDF = key.addKey(superEdgesDF, col(KEYS + "." + key.name))
    }

    // Transform result to vertex and property maps
    val (superEdges, superEdgeProperties) = splitEdgeMap(superEdgesDF.mapValues(_.drop(KEYS)))

    factory.create(superVertices, superEdges, superVertexProperties, superEdgeProperties)
  }

  /** Transforms the given columns to a property map.
   *
   * Any previous property map is overwritten.
   * If the column list is empty, an empty property map is added.
   *
   * @param dataMap dataframe with given columns
   * @param columns column expressions used for columns
   * @return dataframe with given columns moved to properties map
   */
  private def columnsToProperties(dataMap: Map[String, DataFrame], columns: Seq[Column]): Map[String, DataFrame] = {
    if(columns.isEmpty) {
      dataMap.mapValues(_.withColumn(ColumnNames.PROPERTIES, typedLit(Map.empty[String, PropertyValue]))
        .drop(getAlias(defaultAgg)))
    } else {
      val columnNames = columns.map(c => getAlias(c))
      dataMap.mapValues(_.withColumn(ColumnNames.PROPERTIES, map_from_arrays(
        array(columnNames.map(n => lit(n)): _*),
        array(columnNames.map(n => col(n)): _*)))
        .drop(columnNames: _*))
    }
  }

  /** Adds default id, label and graphIds to dataframe.
   *
   * @param dataMap dataframe
   * @return dataframe with new id, default label and empty graph ids
   */
  private def addDefaultColumns(dataMap: Map[String, DataFrame]): Map[String, DataFrame] = {
    dataMap.transform((l, df) => df.select(df("*"),
      longToId(monotonically_increasing_id()).as(ColumnNames.ID),
      lit(l).as(ColumnNames.LABEL),
      typedLit[Array[GradoopId]](Array.empty).as(ColumnNames.GRAPH_IDS)
    ))
  }
}

object TflGrouping {

  def apply[L <: Tfl[L]](builder: GroupingBuilder): TflGrouping[L] = {
    new TflGrouping[L](builder.vertexGroupingKeys, builder.vertexAggFunctions,
      builder.edgeGroupingKeys, builder.edgeAggFunctions)
  }
}
