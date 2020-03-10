package org.gradoop.spark.model.impl.operators.changelayout

import org.apache.spark.sql.{Dataset, Encoder}
import org.gradoop.common.model.api.elements.MainElement
import org.gradoop.spark.expressions.FilterExpressions
import org.gradoop.spark.io.impl.metadata.MetaData
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.graph.BaseGraph
import org.gradoop.spark.model.api.layouts.tfl.TflBaseLayoutFactory
import org.gradoop.spark.model.api.operators.{UnaryGraphCollectionToValueOperator, UnaryLogicalGraphToValueOperator}
import org.gradoop.spark.model.impl.types.{Gve, Tfl}
import org.gradoop.spark.util.TflFunctions

class GveToTfl[L1 <: Gve[L1], L2 <: Tfl[L2]](tflConfig: GradoopSparkConfig[L2],
  graphLabelsOpt: Option[Iterable[String]],
  vertexLabelsOpt: Option[Iterable[String]],
  edgeLabelsOpt: Option[Iterable[String]])
  extends UnaryLogicalGraphToValueOperator[L1#LG, L2#LG] with UnaryGraphCollectionToValueOperator[L1#GC, L2#GC] {

  override def execute(graph: L1#LG): L2#LG = {
    toTfl(graph.layout, tflConfig.logicalGraphFactory)
  }

  override def execute(graph: L1#GC): L2#GC = {
    toTfl(graph.layout, tflConfig.graphCollectionFactory)
  }

  private def toTfl[BG <: BaseGraph[L2]](layout: L1#L, tflFactory: TflBaseLayoutFactory[L2, BG]): BG = {
    import tflConfig.Implicits._
    import tflFactory.Implicits._

    // Split single gve dataset in maps [label -> dataset]
    val graphHeadMap = datasetToMap(layout.graphHeads, graphLabelsOpt)
    val vertexMap = datasetToMap(layout.vertices, vertexLabelsOpt)
    val edgeMap = datasetToMap(layout.edges, edgeLabelsOpt)

    // Transform gve map to two tfl maps (element, properties)
    val (resGrap, resGrapProp) = TflFunctions.splitGraphHeadMap(graphHeadMap.mapValues(_.toDF))
    val (resVert, resVertProp) = TflFunctions.splitVertexMap(vertexMap.mapValues(_.toDF))
    val (resEdge, resEdgeProp) = TflFunctions.splitEdgeMap(edgeMap.mapValues(_.toDF))

    tflFactory.init(resGrap, resVert, resEdge, resGrapProp, resVertProp, resEdgeProp)
  }

  private def datasetToMap[A <: MainElement](dataset: Dataset[A], labelsOpt: Option[Iterable[String]])
    (implicit encoder: Encoder[String]): Map[String, Dataset[A]] = {
    import org.gradoop.spark.util.Implicits._

    val labels: Iterable[String] = labelsOpt.getOrElse(
      dataset.select(dataset.label).distinct.collect)

    labels.map(l => (l, dataset.filter(FilterExpressions.hasLabel(l)))).toMap
  }
}

object GveToTfl {

  def apply[L1 <: Gve[L1], L2 <: Tfl[L2]](tflConfig: GradoopSparkConfig[L2]): GveToTfl[L1, L2] = {
    new GveToTfl[L1, L2](tflConfig, None, None, None)
  }

  def apply[L1 <: Gve[L1], L2 <: Tfl[L2]](tflConfig: GradoopSparkConfig[L2], metaData: MetaData): GveToTfl[L1, L2] = {
    import tflConfig.sparkSession.implicits._
    val graphHeadLabels = metaData.graphHeadMetaData.map(_.label).collect
    val vertexLabels = metaData.vertexMetaData.map(_.label).collect
    val edgeLabels = metaData.edgeMetaData.map(_.label).collect
    new GveToTfl[L1, L2](tflConfig, Some(graphHeadLabels), Some(vertexLabels), Some(edgeLabels))
  }
}
