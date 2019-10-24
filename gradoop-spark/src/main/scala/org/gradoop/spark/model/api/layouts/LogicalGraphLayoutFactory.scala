package org.gradoop.spark.model.api.layouts

import org.apache.spark.sql.Dataset
import org.gradoop.common.model.api.elements.{Edge, GraphHead, Vertex}
import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.elements.{GraphHead, Vertex}
import org.gradoop.spark.model.api.graph.{GraphCollection, LogicalGraph}

abstract class LogicalGraphLayoutFactory[G <: GraphHead, V <: Vertex, E <: Edge, LG <: LogicalGraph, GC <: GraphCollection] extends BaseLayoutFactory[G, V, E, LG, GC] {

  /**
   * Creates a logical graph from the given vertices.
   *
   * @param vertices the vertex Dataset
   * @return Logical graph
   */
  def fromDatasets(vertices: Dataset[V])(implicit config: GradoopSparkConfig[G, V, E, LG, GC]): LG

  /**
   * Creates a logical graph from the given vertices and edges.
   *
   * The method creates a new graph head element and assigns the vertices and
   * edges to that graph.
   *
   * @param vertices Vertex Dataset
   * @param edges    Edge Dataset
   * @return Logical graph
   */
  def fromDatasets(vertices: Dataset[V], edges: Dataset[E])(implicit config: GradoopSparkConfig[G, V, E, LG, GC]): LG

  /**
   * Creates a logical graph from the given graph head, vertices and edges.
   *
   * The method assumes that the given vertices and edges are already assigned
   * to the given graph head.
   *
   * @param graphHead 1-element EPGMGraphHead Dataset
   * @param vertices  Vertex Dataset
   * @param edges     Edge Dataset
   * @return Logical graph
   */
  def fromDatasets(graphHead: Dataset[G], vertices: Dataset[V], edges: Dataset[E])(implicit config: GradoopSparkConfig[G, V, E, LG, GC]): LG

  /**
   * Creates a logical graph from the given Datasets. A new graph head is created and all vertices
   * and edges are assigned to that graph head.
   *
   * @param vertices label indexed vertex Datasets
   * @param edges    label indexed edge Datasets
   * @return Logical graph
   */
  def fromIndexedDatasets(vertices: Map[String, Dataset[V]], edges: Map[String, Dataset[E]])(implicit config: GradoopSparkConfig[G, V, E, LG, GC]): LG

  /**
   * Creates a logical graph from the given Datasets. The method assumes that all vertices and
   * edges are already assigned to the specified graph head.
   *
   * @param graphHead label indexed graph head Dataset (1-element)
   * @param vertices  label indexed vertex Datasets
   * @param edges     label indexed edge Datasets
   * @return Logical graph
   */
  def fromIndexedDatasets(graphHead: Map[String, Dataset[G]], vertices: Map[String, Dataset[V]], edges: Map[String, Dataset[E]])(implicit config: GradoopSparkConfig[G, V, E, LG, GC]): LG

  /**
   * Creates a logical graph from the given single graph head, vertex and edge collections.
   *
   * @param graphHead Graph head associated with the logical graph
   * @param vertices  Vertex collection
   * @param edges     Edge collection
   * @return Logical graph
   */
  def fromCollections(graphHead: G, vertices: Seq[V], edges: Seq[E])(implicit config: GradoopSparkConfig[G, V, E, LG, GC]): LG

  /**
   * Creates a logical graph from the given vertex and edge collections. A new graph head is
   * created and all vertices and edges are assigned to that graph.
   *
   * @param vertices Vertex collection
   * @param edges    Edge collection
   * @return Logical graph
   */
  def fromCollections(vertices: Seq[V], edges: Seq[E])(implicit config: GradoopSparkConfig[G, V, E, LG, GC]): LG

  /**
   * Creates an empty graph.
   *
   * @return empty graph
   */
  def createEmptyGraph(implicit config: GradoopSparkConfig[G, V, E, LG, GC]): LG
}
