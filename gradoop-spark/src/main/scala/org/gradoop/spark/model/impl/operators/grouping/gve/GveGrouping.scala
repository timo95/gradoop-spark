package org.gradoop.spark.model.impl.operators.grouping.gve

import org.apache.spark.sql.Column
import org.gradoop.common.id.GradoopId
import org.gradoop.common.properties.PropertyValue
import org.gradoop.common.util.{ColumnNames, GradoopConstants}
import org.gradoop.spark.model.api.operators.UnaryLogicalGraphToLogicalGraphOperator
import org.gradoop.spark.model.impl.operators.grouping.{GroupingBuilder, GroupingUtil}
import org.gradoop.spark.model.impl.types.Gve

class GveGrouping[L <: Gve[L]](vertexGroupingKeys: Seq[Column], vertexAggFunctions: Seq[Column],
  edgeGroupingKeys: Seq[Column], edgeAggFunctions: Seq[Column])
  extends UnaryLogicalGraphToLogicalGraphOperator[L#LG] {

  override def execute(graph: L#LG): L#LG = {
    val config = graph.config
    implicit val sparkSession = config.sparkSession
    import org.apache.spark.sql.functions._
    val factory = graph.factory
    import factory.Implicits._

    // Column Constants
    val KEYS = "groupingKeys"
    val SUPER_ID = "superId"
    val VERTEX_ID = "vertexId"

    // UDFs
    val newId = udf(() => GradoopId.get) // Run .cache after each use [SPARK-11469]
    val emptyIdSet = udf(() => Array.empty[GradoopId])
    val toProp = udf((v: Any) => PropertyValue(v))

    // Aggregation columns (native elements + functions), id is extra (because of agg syntax)
    val edgeAggColumns = edgeAggFunctions
    val vertexAggColumns = vertexAggFunctions

    // Names of aggregation functions
    val vertexKeyNames = vertexGroupingKeys.map(c => GroupingUtil.getAlias(c))
    val edgeKeyNames = edgeGroupingKeys.map(c => GroupingUtil.getAlias(c))
    val vertexAggNames = vertexAggFunctions.map(c => GroupingUtil.getAlias(c))
    val edgeAggNames = edgeAggFunctions.map(c => GroupingUtil.getAlias(c))

    // TODO what about empty agg lists?


    // ----- Vertices -----

    // Compute vertex grouping keys
    val vertexKeys: Seq[Column] = if(vertexGroupingKeys.isEmpty) Seq(lit(true)) else vertexGroupingKeys
    val vertWithKeys = graph.vertices.withColumn(KEYS, struct(vertexKeys: _*))

    // Group and aggregate vertices
    val superVerticesDF = vertWithKeys.groupBy(KEYS)
      .agg(vertexAggColumns.head, vertexAggColumns.drop(1): _*)
      .withColumn(ColumnNames.ID, newId()).cache // Prevents multiple evaluations of newId [SPARK-11469]

    // Transform result to vertex, add grouping keys and aggregates as properties
    val superVertices = superVerticesDF
      .withColumn(ColumnNames.LABEL, lit(GradoopConstants.DEFAULT_GRAPH_LABEL))
      .withColumn(ColumnNames.GRAPH_IDS, emptyIdSet())
      .withColumn(ColumnNames.PROPERTIES, map_from_arrays(
        array((vertexAggNames ++ vertexKeyNames).map(n => lit(n)): _*),
        array((vertexAggNames.map(n => toProp(col(n))) ++ vertexKeyNames.map(n => col(KEYS + "." + n))): _*)))
      .drop(vertexAggNames: _*)
      .drop(KEYS)
      .as[L#V]

    // Extract vertex id -> superId mapping (needed for edges)
    val vertIdMap = vertWithKeys.select(KEYS, ColumnNames.ID)
      .join(superVerticesDF.select(col(KEYS), col(ColumnNames.ID).as(SUPER_ID)), KEYS)
      .select(ColumnNames.ID, SUPER_ID)
      .withColumnRenamed(ColumnNames.ID, VERTEX_ID)

    // ----- Edges -----

    // Compute edge grouping keys (might depend on original source/target ids)
    val edgeKeys: Seq[Column] = if(edgeGroupingKeys.isEmpty) Seq(lit(true)) else edgeGroupingKeys
    val edgesWithKeys = graph.edges.withColumn(KEYS, struct(edgeKeys: _*))

    // Update edges with vertex super ids
    val updatedEdges = edgesWithKeys
      .join(vertIdMap, col(VERTEX_ID) === col(ColumnNames.SOURCE_ID))
      .drop(ColumnNames.SOURCE_ID, VERTEX_ID)
      .withColumnRenamed(SUPER_ID, ColumnNames.SOURCE_ID)
      .join(vertIdMap, col(VERTEX_ID) === col(ColumnNames.TARGET_ID))
      .drop(ColumnNames.TARGET_ID, VERTEX_ID)
      .withColumnRenamed(SUPER_ID, ColumnNames.TARGET_ID)

    // Group and aggregate edges
    val superEdgesDF = updatedEdges
      .groupBy(KEYS, ColumnNames.SOURCE_ID, ColumnNames.TARGET_ID)
      .agg(edgeAggColumns.head, edgeAggColumns.drop(1): _*)
      .withColumn(ColumnNames.ID, newId()).cache // Prevents multiple evaluations of newId [SPARK-11469]

    // Transform result to edge, add grouping keys and aggregates as properties
    val superEdges = superEdgesDF
      .withColumn(ColumnNames.LABEL, lit(GradoopConstants.DEFAULT_GRAPH_LABEL))
      .withColumn(ColumnNames.GRAPH_IDS, emptyIdSet())
      .withColumn(ColumnNames.PROPERTIES, map_from_arrays(
        array((edgeAggNames ++ edgeKeyNames).map(n => lit(n)): _*),
        array((edgeAggNames.map(n => toProp(col(n))) ++ edgeKeyNames.map(n => col(KEYS + "." + n))): _*)))
      .drop(edgeAggNames: _*)
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
