package org.gradoop.spark.model.api.layouts.tfl

import org.apache.spark.sql.{DataFrame, Dataset, Encoder}
import org.gradoop.common.model.api.components.ComponentTypes
import org.gradoop.common.model.api.tfl.TflElementFactoryProvider
import org.gradoop.spark.expressions.transformation.TransformationFunctions
import org.gradoop.spark.model.api.graph.BaseGraph
import org.gradoop.spark.model.api.layouts.{GraphCollectionLayoutFactory, LogicalGraphLayoutFactory}
import org.gradoop.spark.model.impl.types.Tfl
import org.gradoop.spark.util.Implicits

import scala.collection.mutable

trait TflBaseLayoutFactory[L <: Tfl[L], +BG <: BaseGraph[L]] extends LogicalGraphLayoutFactory[L]
  with GraphCollectionLayoutFactory[L]
  with TflElementFactoryProvider[L#G, L#V, L#E, L#P] {

  object Implicits extends Implicits with ComponentTypes {
    // Encoder
    implicit def implicitTflGraphHeadEncoder: Encoder[L#G] = graphHeadEncoder
    implicit def impliticTflVertexEncoder: Encoder[L#V] = vertexEncoder
    implicit def implicitTflEdgeEncoder: Encoder[L#E] = edgeEncoder
    implicit def implicitTflPropertiesEncoder: Encoder[L#P] = propertiesEncoder
  }

  implicit def graphHeadEncoder: Encoder[L#G]

  implicit def vertexEncoder: Encoder[L#V]

  implicit def edgeEncoder: Encoder[L#E]

  implicit def propertiesEncoder: Encoder[L#P]

  def init(graphHeads: Map[String, Dataset[L#G]],
    vertices: Map[String, Dataset[L#V]],
    edges: Map[String, Dataset[L#E]],
    graphHeadProperties: Map[String, Dataset[L#P]],
    vertexProperties: Map[String, Dataset[L#P]],
    edgeProperties: Map[String, Dataset[L#P]]): BG

  def create(vertices: Map[String, Dataset[L#V]],
    edges: Map[String, Dataset[L#E]],
    vertexProperties: Map[String, Dataset[L#P]],
    edgeProperties: Map[String, Dataset[L#P]]): BG = {

    val graphHead = graphHeadFactory.create
    val graphHeads = sparkSession.createDataset[L#G](Seq(graphHead))

    val addToV = TransformationFunctions.addGraphId[L#V](graphHead.id)
    val addToE = TransformationFunctions.addGraphId[L#E](graphHead.id)
    init(Map(graphHead.label -> graphHeads), vertices.mapValues(addToV), edges.mapValues(addToE),
      Map.empty[String, Dataset[L#P]], vertexProperties, edgeProperties)
  }

  def init(tflLayout: TflLayout[L], tflLayouts: TflLayout[L]*): BG = {
    var graphHeads = tflLayout.graphHeads
    var vertices = tflLayout.vertices
    var edges = tflLayout.edges
    var graphHeadProperties = tflLayout.graphHeadProperties
    var vertexProperties = tflLayout.vertexProperties
    var edgeProperties = tflLayout.edgeProperties

    for (tflLayout: TflLayout[L] <- tflLayouts) {
      graphHeads = unionMaps(graphHeads, tflLayout.graphHeads)
      vertices = unionMaps(vertices, tflLayout.vertices)
      edges = unionMaps(edges, tflLayout.edges)
      graphHeadProperties = unionMaps(graphHeadProperties, tflLayout.graphHeadProperties)
      vertexProperties = unionMaps(vertexProperties, tflLayout.vertexProperties)
      edgeProperties = unionMaps(edgeProperties, tflLayout.edgeProperties)
    }

    init(graphHeads, vertices, edges, graphHeadProperties, vertexProperties, edgeProperties)
  }

  def empty: BG = {
    init(Map.empty[String, Dataset[L#G]], Map.empty[String, Dataset[L#V]], Map.empty[String, Dataset[L#E]],
      Map.empty[String, Dataset[L#P]], Map.empty[String, Dataset[L#P]], Map.empty[String, Dataset[L#P]])
  }

  private def unionMaps[A](left: Map[String, Dataset[A]], right: Map[String, Dataset[A]]): Map[String, Dataset[A]] = {
    val result = mutable.Map.empty[String, Dataset[A]]
    left.foreach(l => result.update(l._1, l._2))
    right.foreach(r => result.update(r._1, if(result.contains(r._1)) result(r._1).union(r._2) else r._2))
    result.toMap
  }
}
