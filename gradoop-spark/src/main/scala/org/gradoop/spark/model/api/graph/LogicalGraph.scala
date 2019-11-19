package org.gradoop.spark.model.api.graph

import org.gradoop.spark.model.api.config.GradoopSparkConfig
import org.gradoop.spark.model.api.layouts.{GveBaseLayoutFactory, GveLayout, LogicalGraphLayout, LogicalGraphLayoutFactory}
import org.gradoop.spark.model.impl.types.GveLayoutType

class LogicalGraph[L <: GveLayoutType](layout: GveLayout[L] with LogicalGraphLayout[L], config: GradoopSparkConfig[L])
  extends BaseGraph[L](layout, config) with LogicalGraphOperators[L] {

  override def factory: GveBaseLayoutFactory[L, LogicalGraph[L]] with LogicalGraphLayoutFactory[L] = config.logicalGraphFactory
}

// ---------------------------
// THOUGHT EXPERIMENT
// ---------------------------

/*
trait EPGM {
  type Category <: Labeled
  type Single
  type Multiple <: Attributed
}

trait TPGM extends EPGM {
  override type Single <: Labeled with Timed
}

trait GLayout[Model <: EPGM] {
  type M = Model
  type I = Identifiable
  type EdgeType // source/target ids
  type GraphMember = GraphElement // graph ids

  type LG <: LogicalGraph[_, _, _, _, _]
  type GC <: GraphCollection[_, _, _, _, _]
}

trait GVE[Model <: EPGM] extends GLayout[Model] {
  type G <: I with M#Category with M#Single with M#Multiple
  type V <: I with M#Category with M#Single with M#Multiple with GraphMember
  type E <: I with M#Category with M#Single with M#Multiple with GraphMember with EdgeType
}

trait LabelTable[Model <: EPGM] extends GLayout[Model] {
  type G = Map[M#Category, I with M#Single with M#Multiple]
  type V = Map[M#Category, I with M#Single with M#Multiple]
  type E = Map[M#Category, I with M#Single with M#Multiple]
}

trait PropExtra[Model <: EPGM] extends GLayout[Model]{
  type G = I with M#Category with M#Single
  type V = I with M#Category with M#Single with GraphMember
  type E = I with M#Category with M#Single with GraphMember with EdgeType
  type GP = I with M#Multiple
  type VP = I with M#Multiple
  type EP = I with M#Multiple
}

trait EPGMGVE extends GVE[EPGM {
  type Category = Labeled
  type Single = Labeled
  type Multiple = Attributed}] {
  override type V = EPGMVert
}
trait TPGMGVE extends GVE[TPGM {
  type Category = Labeled
  type Single = Labeled with Timed
  type Multiple = Attributed}] {
  override type V = TPGMVert
}

trait Timed {
  def time: Int
}

trait Vert extends Identifiable with Labeled with Attributed with GraphElement

trait TimedVertex extends Vert with Timed

case class EPGMVert(var id: Id, var label: Label, var graphIds: IdSet, var properties: Properties, var time: Int) extends Vert
case class TPGMVert(var id: Id, var label: Label, var graphIds: IdSet, var properties: Properties, var time: Int) extends TimedVertex


class Grap[L <: GVE[_]](val vert: L#V) {

  def get: EPGMGVE = ddd.get
  val vert2: EPGMGVE#V = EPGMVert(GradoopId.get, "", Set.empty[GradoopId], Map.empty[String, PropertyValue], 32)

  def getTime(implicit ev: L#V <:< Timed): Int = vert.time

  val ddd: Grap[EPGMGVE] = new Grap[EPGMGVE](vert2) // contains other stuff with M

  def somethingthatworkswithTPGMonlyfeinanoperator(implicit ev: L <:< TPGMGVE): Unit = {
    //val asdf: tpgmvariantofwhatever[M] = new tpgmvariantofwhatever[M] // does not work ;( do we need this though?
  }
}

class OtherGrap(val mdm: TPGMGVE#V) extends Grap[TPGMGVE](mdm) {
  type M = TPGMGVE

  //val dddd: M#G = null
}

class tpgmvariantofwhatever[M <: TPGMGVE](vert: M#V) {


  def dd[T](vert: T): T = vert

  val vertAfter: M#V = dd[M#V](vert)
}
*/