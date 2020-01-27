package org.gradoop.spark.model.api.layouts.tfl

import org.apache.spark.sql.{Dataset, Encoder}
import org.gradoop.common.model.api.tfl.TflElementFactoryProvider
import org.gradoop.spark.expressions.transformation.TransformationFunctions
import org.gradoop.spark.model.api.graph.BaseGraph
import org.gradoop.spark.model.api.layouts.{GraphCollectionLayoutFactory, LogicalGraphLayoutFactory}
import org.gradoop.spark.model.impl.types.Tfl

import scala.collection.mutable

trait TflBaseLayoutFactory[L <: Tfl[L], BG <: BaseGraph[L]] extends LogicalGraphLayoutFactory[L]
  with GraphCollectionLayoutFactory[L]
  with TflElementFactoryProvider[L#G, L#V, L#E, L#P] {

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
      graphHeads = mergeMaps(graphHeads, tflLayout.graphHeads)
      vertices = mergeMaps(vertices, tflLayout.vertices)
      edges = mergeMaps(edges, tflLayout.edges)
      graphHeadProperties = mergeMaps(graphHeadProperties, tflLayout.graphHeadProperties)
      vertexProperties = mergeMaps(vertexProperties, tflLayout.vertexProperties)
      edgeProperties = mergeMaps(edgeProperties, tflLayout.edgeProperties)
    }

    init(graphHeads, vertices, edges, graphHeadProperties, vertexProperties, edgeProperties)
  }

  def empty: BG = {
    init(Map.empty[String, Dataset[L#G]], Map.empty[String, Dataset[L#V]], Map.empty[String, Dataset[L#E]],
      Map.empty[String, Dataset[L#P]], Map.empty[String, Dataset[L#P]], Map.empty[String, Dataset[L#P]])
  }

  private def mergeMaps[A](left: Map[String, Dataset[A]], right: Map[String, Dataset[A]]): Map[String, Dataset[A]] = {
    val result = mutable.Map.empty[String, Dataset[A]]
    left.foreach(l => result.update(l._1, l._2))
    right.foreach(r => result.update(r._1, if(result.contains(r._1)) result(r._1).union(r._2) else r._2))
    result.toMap
  }
}
