package org.gradoop.spark.model.impl.operators.changelayout

import org.apache.spark.sql.SparkSession
import org.gradoop.spark.expressions.filter.FilterExpressions
import org.gradoop.spark.io.impl.metadata.MetaData
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.BaseGraph
import org.gradoop.spark.model.api.layouts.gve.GveBaseLayoutFactory
import org.gradoop.spark.model.api.layouts.tfl.TflBaseLayoutFactory
import org.gradoop.spark.model.api.operators.{UnaryGraphCollectionToValueOperator, UnaryLogicalGraphToValueOperator}
import org.gradoop.spark.model.impl.types.{Gve, Tfl}
import org.gradoop.spark.util.TflFunctions

class GveToTfl[L1 <: Gve[L1], L2 <: Tfl[L2]](tflConfig: GradoopSparkConfig[L2],
  graphLabelsOpt: Option[Iterable[String]],
  vertexLabelsOpt: Option[Iterable[String]],
  edgeLabelsOpt: Option[Iterable[String]])
  extends UnaryLogicalGraphToValueOperator[L1#LG, L2#LG] with UnaryGraphCollectionToValueOperator[L1#GC, L2#GC] {
  import tflConfig.sparkSession.implicits._

  override def execute(graph: L1#LG): L2#LG = {
    toTfl(graph.layout, graph.factory, tflConfig.logicalGraphFactory)
  }

  override def execute(graph: L1#GC): L2#GC = {
    toTfl(graph.layout, graph.factory, tflConfig.graphCollectionFactory)
  }

  private def toTfl[BG <: BaseGraph[L2]](layout: L1#L, factory: GveBaseLayoutFactory[L1, _],
    tflFactory: TflBaseLayoutFactory[L2, BG]): BG = {
    import factory.Implicits._

    // Extract labels
    val graphLabels: Iterable[String] = graphLabelsOpt.getOrElse(
      layout.graphHead.select(layout.graphHead.label).distinct.collect)
    val vertexLabels: Iterable[String] = vertexLabelsOpt.getOrElse(
      layout.vertices.select(layout.vertices.label).distinct.collect)
    val edgeLabels: Iterable[String] = edgeLabelsOpt.getOrElse(
      layout.edges.select(layout.edges.label).distinct.collect)

    // Split single gve dataset in maps [label -> dataset]
    val graphHeadMap = graphLabels.map(l =>
      (l, layout.graphHeads.filter(FilterExpressions.hasLabel(l)))).toMap
    val vertexMap = vertexLabels
      .map(l => (l, layout.vertices.filter(FilterExpressions.hasLabel(l)))).toMap
    val edgeMap = edgeLabels.map(l =>
      (l, layout.edges.filter(FilterExpressions.hasLabel(l)))).toMap

    { // Limit scope of implicits (Tfl inside, Gve above)
      // Transform gve map to two tfl maps (element, properties)
      import tflFactory.Implicits._
      val (resGrap, resGrapProp) = TflFunctions.splitGraphHeadMap(graphHeadMap.mapValues(_.toDF))
      val (resVert, resVertProp) = TflFunctions.splitVertexMap(vertexMap.mapValues(_.toDF))
      val (resEdge, resEdgeProp) = TflFunctions.splitEdgeMap(edgeMap.mapValues(_.toDF))

      tflFactory.init(resGrap, resVert, resEdge, resGrapProp, resVertProp, resEdgeProp)
    }
  }
}

object GveToTfl {

  def apply[L1 <: Gve[L1], L2 <: Tfl[L2]](tflConfig: GradoopSparkConfig[L2]): GveToTfl[L1, L2] = {
    new GveToTfl[L1, L2](tflConfig, None, None, None)
  }

  def apply[L1 <: Gve[L1], L2 <: Tfl[L2]](tflConfig: GradoopSparkConfig[L2], metaData: MetaData)
    (implicit sparkSession: SparkSession): GveToTfl[L1, L2] = {
    import sparkSession.implicits._
    val graphHeadLabels = metaData.graphHeadMetaData.map(_.label).collect
    val vertexLabels = metaData.vertexMetaData.map(_.label).collect
    val edgeLabels = metaData.edgeMetaData.map(_.label).collect
    new GveToTfl[L1, L2](tflConfig, Some(graphHeadLabels), Some(vertexLabels), Some(edgeLabels))
  }
}
